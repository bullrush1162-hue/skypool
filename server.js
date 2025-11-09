// SkyPool — Postgres‑backed pooled Google Drives
// Features:
//  - Pooled storage across multiple Google Drive accounts (OAuth)
//  - Auto chunk size (no manual setting)
//  - Simple in‑memory upload queue with progress
//  - Delete files (DB + best‑effort remote chunk deletion)
//  - Read‑only WebDAV to map as a Windows network drive
//  - Admin‑only dashboard with animated "ocean → rivers" UI
//  - Correct filename & Content‑Type on download
//
// Required env (Render/Railway → Environment):
// PORT=8080
// BASE_URL=https://<your-host>
// GOOGLE_CLIENT_ID=...
// GOOGLE_CLIENT_SECRET=...
// GOOGLE_REDIRECT_URI=https://<your-host>/oauth2/callback
// DATABASE_URL=<postgres-connection-string>
// ADMIN_USER=admin
// ADMIN_PASS=change-me
// WEBDAV_USER=skypool
// WEBDAV_PASS=skypool
// (optional) PGSSLMODE=disable   # only if your Postgres does not require SSL
//
// package.json deps:
//   "express", "multer", "pg", "googleapis", "uuid", "dotenv", "basic-auth", "mime-types"
//   (and set  "type":"module"  to allow ES module imports)

import express from 'express';
import multer from 'multer';
import { Pool } from 'pg';
import { google } from 'googleapis';
import { v4 as uuidv4 } from 'uuid';
import fs from 'fs/promises';
import fssync from 'fs';
import path from 'path';
import crypto from 'crypto';
import dotenv from 'dotenv';
import basicAuth from 'basic-auth';
import mime from 'mime-types';

dotenv.config();

const PORT = process.env.PORT || 8080;
const BASE_URL = (process.env.BASE_URL || `http://localhost:${PORT}`).replace(/\/$/, '');
const ADMIN_USER = process.env.ADMIN_USER || 'admin';
const ADMIN_PASS = process.env.ADMIN_PASS || 'change-me';
const DAV_USER = process.env.WEBDAV_USER || 'skypool';
const DAV_PASS = process.env.WEBDAV_PASS || 'skypool';

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) throw new Error('Missing DATABASE_URL env var');
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'disable' ? false : { rejectUnauthorized: false }
});
async function query(q, params) { const { rows } = await pool.query(q, params); return rows; }

// ---------- Migrations ----------
async function migrate() {
  await query(`CREATE TABLE IF NOT EXISTS accounts(
    id TEXT PRIMARY KEY,
    email TEXT,
    display_name TEXT,
    refresh_token TEXT NOT NULL,
    created_at BIGINT
  )`);
  await query(`CREATE TABLE IF NOT EXISTS manifests(
    id TEXT PRIMARY KEY,
    original_name TEXT,
    original_mime TEXT,
    size BIGINT,
    sha256 TEXT,
    chunk_size_mb INT,
    created_at BIGINT
  )`);
  await query(`CREATE TABLE IF NOT EXISTS parts(
    manifest_id TEXT,
    idx INT,
    size BIGINT,
    sha256 TEXT,
    account_id TEXT,
    drive_file_id TEXT,
    drive_file_name TEXT,
    PRIMARY KEY(manifest_id, idx)
  )`);
}

// ---------- Google OAuth helpers ----------
function oauth2Client() {
  return new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    process.env.GOOGLE_REDIRECT_URI
  );
}
const SCOPES = [
  'https://www.googleapis.com/auth/drive.file',
  'https://www.googleapis.com/auth/drive.metadata.readonly',
  'email', 'profile'
];

async function getAccounts() { return await query('SELECT * FROM accounts ORDER BY created_at ASC'); }
async function getAccount(id) { const r = await query('SELECT * FROM accounts WHERE id=$1', [id]); return r[0]; }
async function clientForAccount(accountId) {
  const acc = await getAccount(accountId);
  if (!acc) throw new Error('Account not found');
  const client = oauth2Client();
  client.setCredentials({ refresh_token: acc.refresh_token });
  return { acc, client };
}
async function driveUpload({ client, name, mimeType, data }) {
  const drive = google.drive({ version: 'v3', auth: client });
  const res = await drive.files.create({ requestBody: { name }, media: { mimeType, body: data } }, { headers: { 'X-Upload-Content-Type': mimeType } });
  return res.data; // { id, name }
}
async function driveDelete({ client, fileId }) { const drive = google.drive({ version: 'v3', auth: client }); await drive.files.delete({ fileId }).catch(()=>{}); }

// ---------- Utils ----------
function safeFileName(name) { return (path.basename(name || 'file')).replace(/["\\\r\n]/g, '_') || 'file'; }
function contentTypeFor(name, fallback='application/octet-stream') { return mime.lookup(name) || fallback; }
async function sha256File(filePath) { return await new Promise((resolve, reject) => { const h=crypto.createHash('sha256'); const s=fssync.createReadStream(filePath); s.on('error',reject); s.on('data',d=>h.update(d)); s.on('end',()=>resolve(h.digest('hex'))); }); }
async function splitFileToDir(filePath, chunkSizeMB, outDir) { await fs.mkdir(outDir,{recursive:true}); const stat=await fs.stat(filePath); const chunkSize=Math.max(1,chunkSizeMB)*1024*1024; const base=path.basename(filePath); const parts=[]; await new Promise((resolve,reject)=>{ const rs=fssync.createReadStream(filePath,{highWaterMark:chunkSize}); let idx=0; rs.on('data',(buf)=>{ const name=`${base}.part.${String(idx).padStart(5,'0')}`; const p=path.join(outDir,name); rs.pause(); fssync.writeFileSync(p,buf); const h=crypto.createHash('sha256').update(buf).digest('hex'); parts.push({ idx, path:p, size:buf.length, sha256:h, name }); idx++; rs.resume(); }); rs.on('end',resolve); rs.on('error',reject); }); return { parts, size: stat.size }; }

function adminAuth(req,res,next){ const c=basicAuth(req); if(!c||c.name!==ADMIN_USER||c.pass!==ADMIN_PASS){ res.set('WWW-Authenticate','Basic realm="SkyPool Admin"'); return res.status(401).send('Admin login required'); } next(); }

// ---------- App & upload staging ----------
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
const TMP_UPLOAD_DIR = path.join(process.cwd(),'tmp_uploads');
const TMP_CHUNK_DIR  = path.join(process.cwd(),'tmp_chunks');
await fs.mkdir(TMP_UPLOAD_DIR,{recursive:true}); await fs.mkdir(TMP_CHUNK_DIR,{recursive:true});
const upload = multer({ dest: TMP_UPLOAD_DIR });

// ---------- Simple Queue ----------
const jobs = new Map(); // id -> {id, filename, size, status, progress, error, createdAt, tmpPath}
const queue = [];
let working = false;

function autoChunkMB(bytes, accounts){
  const target = Math.max(6, Math.min(20, (accounts||1)*6));
  const mb = Math.ceil((bytes / target) / (1024*1024));
  return Math.max(64, Math.min(1024, mb));
}

async function runQueue(){ if (working) return; working = true; while(queue.length){ const id = queue.shift(); const job = jobs.get(id); if(!job) continue; try{ job.status='processing'; await processJob(job); job.status='done'; job.progress=100; }catch(e){ job.status='error'; job.error=String(e); } } working=false; }

async function processJob(job){
  const accounts = await getAccounts();
  if (!accounts.length) throw new Error('No accounts connected');
  const tmpPath = job.tmpPath;
  const st = await fs.stat(tmpPath);
  const chunkMB = autoChunkMB(st.size, accounts.length);
  const fileSha = await sha256File(tmpPath);
  const manifestId = uuidv4();
  const tmpDir = path.join(TMP_CHUNK_DIR, manifestId);
  const { parts, size } = await splitFileToDir(tmpPath, chunkMB, tmpDir);
  await query('INSERT INTO manifests(id,original_name,original_mime,size,sha256,chunk_size_mb,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)', [manifestId, job.filename, contentTypeFor(job.filename), size, fileSha, chunkMB, Date.now()]);
  let aIdx=0, uploaded=0; for (const p of parts){ const acc = accounts[aIdx % accounts.length]; const { client } = await clientForAccount(acc.id); const meta = await driveUpload({ client, name: p.name, mimeType: 'application/octet-stream', data: fssync.createReadStream(p.path) }); await query('INSERT INTO parts(manifest_id,idx,size,sha256,account_id,drive_file_id,drive_file_name) VALUES($1,$2,$3,$4,$5,$6,$7)', [manifestId, p.idx, p.size, p.sha256, acc.id, meta.id, meta.name || p.name]); aIdx++; uploaded++; job.progress = Math.round((uploaded/parts.length)*100); }
  await fs.rm(tmpPath).catch(()=>{}); await fs.rm(tmpDir,{recursive:true,force:true}).catch(()=>{});
}

// ---------- HTML helpers (avoid nested template literals) ----------
function renderJobItem(j){
  return '<li class="item">' +
    '<div>' + safe(j.filename) + ' <span class="mut">(' + (j.size/1e6).toFixed(1) + ' MB)</span> — <b>' + safe(j.status) + '</b></div>' +
    '<div class="bar"><i style="width:' + (j.progress||0) + '%"></i></div>' +
    (j.error ? '<div class="mut">' + safe(j.error) + '</div>' : '') +
  '</li>';
}
function renderFileItem(x){
  const href = BASE_URL + '/download?manifestId=' + encodeURIComponent(x.id);
  return '<li class="item">' +
    '<div>' + safe(safeFileName(x.original_name)) + ' <span class="mut">' + (Number(x.size)/1e9).toFixed(3) + ' GB</span></div>' +
    '<div style="display:flex;gap:10px;margin-top:6px">' +
      '<a class="btn sub" href="' + href + '">download</a>' +
      '<form method="post" action="/delete" onsubmit="return confirm(\'Delete this file?\')">' +
      '<input type="hidden" name="manifestId" value="' + x.id + '"/>' +
      '<button class="btn sub" type="submit">delete</button>' +
      '</form>' +
    '</div>' +
  '</li>';
}
function safe(s){ return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[c])); }

// ---------- UI (animated ocean → rivers) ----------
app.get('/', adminAuth, async (req,res)=>{
  const accounts = await getAccounts();
  const files = await query('SELECT * FROM manifests ORDER BY created_at DESC LIMIT 200');
  const jobsArr = [...jobs.values()].sort((a,b)=>b.createdAt-a.createdAt).slice(0,50);

  const jobsHtml = jobsArr.map(renderJobItem).join('') || '<i class="mut">No jobs yet</i>';
  const filesHtml = files.map(renderFileItem).join('') || '<i class="mut">No files yet</i>';

  res.type('html').send(`<!doctype html>
  <html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>SkyPool</title>
  <style>
    :root{--bg:#0b1220;--card:#12192a;--mut:#92a0b5;--txt:#e8f0ff;--acc:#4da3ff}
    body{margin:0;background:linear-gradient(180deg,#08101e,#0b1220 30%,#0b1220);color:var(--txt);font:16px system-ui,Segoe UI,Roboto,Arial}
    header{position:relative;overflow:hidden;border-bottom:1px solid #0f1b33}
    .wrap{max-width:980px;margin:0 auto;padding:20px}
    .grid{display:grid;grid-template-columns:1.2fr .8fr;gap:16px}
    .card{background:var(--card);border:1px solid #0f1b33;border-radius:16px;padding:16px}
    h1{margin:0 0 10px;font-weight:800;letter-spacing:.5px}
    label,input,button{font:inherit}
    .btn{background:var(--acc);color:#001b33;font-weight:700;border:0;border-radius:12px;padding:10px 14px;cursor:pointer}
    .btn.sub{background:#1a2742;color:var(--txt)}
    .mut{color:var(--mut)}
    .list{list-style:none;padding:0;margin:0}
    .item{padding:10px 0;border-bottom:1px solid #0f233f}
    .bar{height:10px;background:#0f233f;border-radius:999px;overflow:hidden}
    .bar>i{display:block;height:10px;background:var(--acc);width:0%}
    .kv{display:flex;gap:12px;flex-wrap:wrap;font-size:14px;color:var(--mut)}
    .pill{display:inline-block;background:#0e1d35;border:1px solid #1e335a;border-radius:999px;padding:6px 10px;margin:4px 6px 0 0}
    .ocean{position:relative;height:220px;background:radial-gradient(1200px 300px at 50% -50px,#14305e,#0b1220 70%)}
    .river{position:absolute;inset:0;pointer-events:none}
    .river path{fill:none;stroke:#4da3ff55;stroke-width:3;stroke-linecap:round;stroke-linejoin:round;stroke-dasharray:4 10;animation:flow 2.2s linear infinite}
    .river path:nth-child(2){animation-duration:1.8s}
    .river path:nth-child(3){animation-duration:2.6s}
    @keyframes flow{to{stroke-dashoffset:-200}}
    .titleGlow{position:absolute;inset:0;display:flex;align-items:center;justify-content:center}
    .titleGlow h1{font-size:42px;text-shadow:0 0 18px #4da3ff55}
  </style></head>
  <body>
    <header>
      <div class="ocean">
        <svg class="river" viewBox="0 0 1200 220" preserveAspectRatio="none">
          <path d="M0 40 C 200 60, 300 20, 500 50 S 900 80, 1200 40"/>
          <path d="M0 100 C 200 120, 300 80, 500 110 S 900 140, 1200 100"/>
          <path d="M0 160 C 200 180, 300 140, 500 170 S 900 200, 1200 160"/>
        </svg>
        <div class="titleGlow"><h1>SkyPool</h1></div>
      </div>
      <div class="wrap kv">
        <span class="pill">Accounts: ${accounts.length}</span>
        <span class="pill" id="jobCount">Jobs: ${jobsArr.length}</span>
        <span class="pill">Files: ${files.length}</span>
        <a class="pill" href="/oauth2/start" style="text-decoration:none;color:inherit">+ Add Google Account</a>
      </div>
    </header>

    <main class="wrap grid">
      <section class="card">
        <h3>Upload to pool</h3>
        <form action="/upload" method="post" enctype="multipart/form-data">
          <input type="file" name="file" required />
          <button class="btn" type="submit">Enqueue upload</button>
          <span class="mut">Auto chunk size — optimized per file</span>
        </form>
        <h4 style="margin-top:18px">Queue</h4>
        <ul id="jobs" class="list">${jobsHtml}</ul>
      </section>

      <section class="card">
        <h3>Files</h3>
        <ul class="list">${filesHtml}</ul>
        <p class="mut" style="margin-top:10px">WebDAV (read‑only): <code>${BASE_URL}/dav</code> — user: <b>${DAV_USER}</b>, pass: <b>${DAV_PASS}</b></p>
      </section>
    </main>

    <script>
      async function refreshJobs(){
        try{ const r = await fetch('/api/jobs'); if(!r.ok) return; const data = await r.json(); const list = document.getElementById('jobs'); const html = data.map(j=>(
          '<li class="item">' +
          '<div>' + j.filename.replace(/[&<>"\']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[s])) + ' <span class="mut">(' + (j.size/1e6).toFixed(1) + ' MB)</span> — <b>' + j.status + '</b></div>' +
          '<div class="bar"><i style="width:' + (j.progress||0) + '%"></i></div>' + (j.error?('<div class="mut">'+j.error+'</div>'):'') + '</li>'
        )).join('') || '<i class="mut">No jobs</i>'; list.innerHTML = html; document.getElementById('jobCount').textContent = 'Jobs: ' + data.length; }catch(e){}
      }
      setInterval(refreshJobs, 1500);
    </script>
  </body></html>`);
});

// Jobs API
app.get('/api/jobs', adminAuth, (req,res)=>{
  const arr = [...jobs.values()].sort((a,b)=>b.createdAt-a.createdAt).slice(0,100).map(j=>({ id:j.id, filename:j.filename, size:j.size, status:j.status, progress:j.progress||0, error:j.error||null }));
  res.json(arr);
});

// OAuth
app.get('/oauth2/start', (req,res)=>{ const c=oauth2Client(); const url=c.generateAuthUrl({ access_type:'offline', prompt:'consent', scope:SCOPES }); res.redirect(url); });
app.get('/oauth2/callback', async (req,res)=>{ try{ const c=oauth2Client(); const { code } = req.query; const { tokens } = await c.getToken(String(code)); if(!tokens.refresh_token) throw new Error('No refresh_token'); c.setCredentials(tokens); const oauth2 = google.oauth2({ version:'v2', auth:c }); const me = await oauth2.userinfo.get(); const email = me.data.email || 'unknown@unknown'; const name = me.data.name || email; const id = uuidv4(); await query('INSERT INTO accounts(id,email,display_name,refresh_token,created_at) VALUES($1,$2,$3,$4,$5)', [id,email,name,tokens.refresh_token,Date.now()]); res.redirect('/'); }catch(e){ res.status(500).send(String(e)); } });

// Upload → enqueue job
app.post('/upload', adminAuth, upload.single('file'), async (req,res)=>{ try{ const st = await fs.stat(req.file.path); const id = uuidv4(); jobs.set(id, { id, filename: safeFileName(req.file.originalname||'file'), size: st.size, status:'queued', progress:0, createdAt: Date.now(), tmpPath: req.file.path }); queue.push(id); runQueue(); res.redirect('/'); }catch(e){ res.status(500).send(String(e)); } });

// Delete
app.post('/delete', adminAuth, express.urlencoded({extended:true}), async (req,res)=>{ const id = String(req.body.manifestId||''); try{ const m = (await query('SELECT * FROM manifests WHERE id=$1',[id]))[0]; if(!m) return res.status(404).send('Manifest not found'); const parts = await query('SELECT * FROM parts WHERE manifest_id=$1 ORDER BY idx ASC',[id]); for(const p of parts){ try{ const { client } = await clientForAccount(p.account_id); await driveDelete({ client, fileId: p.drive_file_id }); }catch{} } await pool.query('BEGIN'); await pool.query('DELETE FROM parts WHERE manifest_id=$1',[id]); await pool.query('DELETE FROM manifests WHERE id=$1',[id]); await pool.query('COMMIT'); res.redirect('/'); }catch(e){ try{ await pool.query('ROLLBACK'); }catch{} res.status(500).send(String(e)); } });

// Download (assembled)
app.get('/download', async (req,res)=>{ try{ const id=String(req.query.manifestId); const m=(await query('SELECT * FROM manifests WHERE id=$1',[id]))[0]; if(!m) return res.status(404).send('Manifest not found'); const name=safeFileName(m.original_name||'file'); const ctype=m.original_mime||contentTypeFor(name); res.setHeader('Content-Type',ctype); res.setHeader('Content-Disposition',`attachment; filename="${name}"; filename*=UTF-8''${encodeURIComponent(name)}`); const parts=await query('SELECT * FROM parts WHERE manifest_id=$1 ORDER BY idx ASC',[id]); for(const p of parts){ const { client } = await clientForAccount(p.account_id); const drive=google.drive({version:'v3',auth:client}); const dl=await drive.files.get({ fileId:p.drive_file_id, alt:'media' }, { responseType:'stream' }); await new Promise((resolve,reject)=>{ dl.data.on('error',reject); dl.data.on('end',resolve); dl.data.pipe(res,{end:false}); }); } res.end(); }catch(e){ console.error(e); res.status(500).send(String(e)); } });

// WebDAV (read‑only)
function davAuth(req,res,next){ const c=basicAuth(req); if(!c||c.name!==DAV_USER||c.pass!==DAV_PASS){ res.set('WWW-Authenticate','Basic realm="SkyPool DAV"'); return res.status(401).send('Auth required'); } next(); }
function xmlMultiStatus(items){ return `<?xml version="1.0" encoding="utf-8"?>\n<d:multistatus xmlns:d="DAV:">${items.join('')}</d:multistatus>`; }
function xmlResponse(href,isCollection,size){ return `\n<d:response>\n  <d:href>${href}</d:href>\n  <d:propstat>\n    <d:prop>\n      <d:resourcetype>${isCollection?'<d:collection/>':''}</d:resourcetype>\n      ${!isCollection?`<d:getcontentlength>${size}</d:getcontentlength>`:''}\n    </d:prop>\n    <d:status>HTTP/1.1 200 OK</d:status>\n  </d:propstat>\n</d:response>`; }

app.options('/dav*', davAuth, (req,res)=>{ res.set({ 'DAV':'1,2', 'Allow':'OPTIONS, PROPFIND, GET', 'MS-Author-Via':'DAV' }); res.status(200).end(); });
app.all('/dav*', davAuth);
app.all('/dav', davAuth, async (req,res,next)=>{ if ((req.method||'').toUpperCase()==='PROPFIND') return next(); res.status(405).end(); });
app.all('/dav/:manifestId', davAuth, async (req,res,next)=>{ if ((req.method||'').toUpperCase()==='PROPFIND') return next(); res.status(405).end(); });

app.all('/dav', davAuth, async (req,res)=>{ const depth=req.headers.depth||'1'; const base=`${BASE_URL}/dav`; const mf=await query('SELECT id, original_name, size FROM manifests ORDER BY created_at DESC'); const items=[ xmlResponse(base+'/',true) ]; if(depth!=='0') for(const m of mf) items.push(xmlResponse(`${base}/${encodeURIComponent(m.id)}/`,true)); res.type('application/xml').status(207).send(xmlMultiStatus(items)); });
app.all('/dav/:manifestId', davAuth, async (req,res)=>{ const id=req.params.manifestId; const base=`${BASE_URL}/dav/${encodeURIComponent(id)}`; const m=(await query('SELECT id, original_name, size FROM manifests WHERE id=$1',[id]))[0]; if(!m) return res.status(404).end(); const items=[ xmlResponse(base+'/',true) ]; const fname=safeFileName(m.original_name); items.push(xmlResponse(`${base}/${encodeURIComponent(fname)}`,false,m.size)); res.type('application/xml').status(207).send(xmlMultiStatus(items)); });
app.get('/dav/:manifestId/:filename', davAuth, async (req,res)=>{ try{ const id=req.params.manifestId; const m=(await query('SELECT * FROM manifests WHERE id=$1',[id]))[0]; if(!m) return res.status(404).send('Not found'); const name=safeFileName(m.original_name||req.params.filename); const ctype=m.original_mime||contentTypeFor(name); res.setHeader('Content-Type',ctype); res.setHeader('Content-Disposition',`inline; filename="${name}"; filename*=UTF-8''${encodeURIComponent(name)}`); const parts=await query('SELECT * FROM parts WHERE manifest_id=$1 ORDER BY idx ASC',[id]); for(const p of parts){ const { client } = await clientForAccount(p.account_id); const drive=google.drive({version:'v3',auth:client}); const dl=await drive.files.get({ fileId:p.drive_file_id, alt:'media' }, { responseType:'stream' }); await new Promise((resolve,reject)=>{ dl.data.on('error',reject); dl.data.on('end',resolve); dl.data.pipe(res,{end:false}); }); } res.end(); }catch(e){ console.error(e); res.status(500).send(String(e)); } });

await migrate();
app.listen(PORT, () => console.log(`SkyPool running on ${BASE_URL} — WebDAV at /dav`));
