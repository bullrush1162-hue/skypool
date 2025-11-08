// server.js (SkyPool — refined)
// - Keeps original filename & MIME type on download
// - Shows the same filename in Web UI, /download and WebDAV
// - Minor hardening for headers and encoding
//
// Deps:
//   npm i express multer better-sqlite3 googleapis uuid dotenv basic-auth mime-types

import express from 'express';
import multer from 'multer';
import Database from 'better-sqlite3';
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
const DAV_USER = process.env.WEBDAV_USER || 'skypool';
const DAV_PASS = process.env.WEBDAV_PASS || 'skypool';

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// temp dirs
const TMP_UPLOAD_DIR = path.join(process.cwd(), 'tmp_uploads');
const TMP_CHUNK_DIR = path.join(process.cwd(), 'tmp_chunks');
const TMP_RESTORE_DIR = path.join(process.cwd(), 'tmp_restore');
await fs.mkdir(TMP_UPLOAD_DIR, { recursive: true });
await fs.mkdir(TMP_CHUNK_DIR, { recursive: true });
await fs.mkdir(TMP_RESTORE_DIR, { recursive: true });

// Multer to accept one file
const upload = multer({ dest: TMP_UPLOAD_DIR });

// DB setup
const db = new Database('distfs.db');
db.pragma('journal_mode = WAL');
db.exec(`
CREATE TABLE IF NOT EXISTS accounts (
  id TEXT PRIMARY KEY,
  email TEXT,
  display_name TEXT,
  refresh_token TEXT NOT NULL,
  created_at INTEGER
);
CREATE TABLE IF NOT EXISTS manifests (
  id TEXT PRIMARY KEY,
  original_name TEXT,
  original_mime TEXT,
  size INTEGER,
  sha256 TEXT,
  chunk_size_mb INTEGER,
  created_at INTEGER
);
CREATE TABLE IF NOT EXISTS parts (
  manifest_id TEXT,
  idx INTEGER,
  size INTEGER,
  sha256 TEXT,
  account_id TEXT,
  drive_file_id TEXT,
  drive_file_name TEXT,
  PRIMARY KEY (manifest_id, idx)
);
`);

// Google OAuth client
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
  'email',
  'profile'
];

function getAccounts() {
  return db.prepare('SELECT * FROM accounts').all();
}

async function clientForAccount(accountId) {
  const acc = db.prepare('SELECT * FROM accounts WHERE id=?').get(accountId);
  if (!acc) throw new Error('Account not found');
  const client = oauth2Client();
  client.setCredentials({ refresh_token: acc.refresh_token });
  return { acc, client };
}

async function driveUpload({ client, name, mimeType, data }) {
  const drive = google.drive({ version: 'v3', auth: client });
  const res = await drive.files.create({
    requestBody: { name },
    media: { mimeType, body: data }
  }, { headers: { 'X-Upload-Content-Type': mimeType } });
  return res.data; // { id, name }
}

function safeFileName(name) {
  // Avoid header injection & odd characters; keep extension
  const base = path.basename(name).replace(/["\\\r\n]/g, '_');
  return base || 'file';
}

function contentTypeFor(name, fallback = 'application/octet-stream') {
  return mime.lookup(name) || fallback;
}

async function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const s = fssync.createReadStream(filePath);
    s.on('error', reject);
    s.on('data', d => hash.update(d));
    s.on('end', () => resolve(hash.digest('hex')));
  });
}

async function splitFileToDir(filePath, chunkSizeMB, outDir) {
  await fs.mkdir(outDir, { recursive: true });
  const stat = await fs.stat(filePath);
  const chunkSize = Math.max(1, chunkSizeMB) * 1024 * 1024;
  const base = path.basename(filePath);
  const parts = [];
  await new Promise((resolve, reject) => {
    const rs = fssync.createReadStream(filePath, { highWaterMark: chunkSize });
    let idx = 0;
    rs.on('data', (buf) => {
      const name = `${base}.part.${String(idx).padStart(5, '0')}`;
      const p = path.join(outDir, name);
      rs.pause();
      fssync.writeFileSync(p, buf);
      const h = crypto.createHash('sha256').update(buf).digest('hex');
      parts.push({ idx, path: p, size: buf.length, sha256: h, name });
      idx++;
      rs.resume();
    });
    rs.on('end', resolve);
    rs.on('error', reject);
  });
  return { parts, baseName: base, size: stat.size };
}

// ---------------- UI ----------------
app.get('/', async (req, res) => {
  const accounts = getAccounts();
  const usages = [];
  for (const a of accounts) {
    try {
      const { client } = await clientForAccount(a.id);
      const drive = google.drive({ version: 'v3', auth: client });
      const about = await drive.about.get({ fields: 'storageQuota,user' });
      usages.push({
        id: a.id,
        email: a.email,
        name: a.display_name || a.email,
        limit: Number(about.data.storageQuota?.limit || 0),
        usage: Number(about.data.storageQuota?.usage || 0)
      });
    } catch {
      usages.push({ id: a.id, email: a.email, name: a.display_name || a.email, limit: 0, usage: 0 });
    }
  }
  const totalLimit = usages.reduce((s, u) => s + (u.limit || 0), 0);
  const totalUsage = usages.reduce((s, u) => s + (u.usage || 0), 0);
  const m = db.prepare('SELECT * FROM manifests ORDER BY created_at DESC LIMIT 200').all();

  const listItems = m.map(x => {
    const href = `${BASE_URL}/download?manifestId=${encodeURIComponent(x.id)}`;
    return `<li>${safeFileName(x.original_name)} — ${(x.size/1e9).toFixed(3)} GB — <a href="${href}">download</a></li>`;
  }).join('') || '<i>none yet</i>';

  res.type('html').send(`
  <html><head><title>DistFS (SkyPool)</title><meta name="viewport" content="width=device-width,initial-scale=1"/>
  <style>body{font-family:system-ui,Arial;margin:20px;max-width:900px} .card{padding:14px;border:1px solid #eee;border-radius:12px;margin:10px 0}</style></head>
  <body>
    <h1>DistFS (pooled Google Drives)</h1>
    <div class="card">
      <h3>Accounts: ${accounts.length}</h3>
      <p>Total capacity: ${totalLimit ? (totalLimit/1e9).toFixed(1)+' GB' : 'unknown'} | Used: ${(totalUsage/1e9).toFixed(1)} GB</p>
      <ul>${usages.map(u => `<li>${u.name} — used ${(u.usage/1e9).toFixed(1)} GB / ${u.limit? (u.limit/1e9).toFixed(1)+' GB' : 'unknown'}</li>`).join('')}</ul>
      <a href="/oauth2/start"><button>Add Google Account</button></a>
    </div>

    <div class="card">
      <h3>Upload</h3>
      <form action="/upload" method="post" enctype="multipart/form-data">
        <input type="file" name="file" required />
        <label> Chunk (MB) <input type="number" name="chunk" value="512" min="64" step="64"/></label>
        <button type="submit">Upload</button>
      </form>
    </div>

    <div class="card">
      <h3>Files</h3>
      <ul>${listItems}</ul>
      <p>WebDAV (read-only): <code>${BASE_URL}/dav</code> — user: <b>${DAV_USER}</b>, pass: <b>${DAV_PASS}</b></p>
    </div>
  </body></html>`);
});

// ---------------- OAuth ----------------
app.get('/oauth2/start', (req, res) => {
  const client = oauth2Client();
  const url = client.generateAuthUrl({ access_type: 'offline', prompt: 'consent', scope: SCOPES });
  res.redirect(url);
});

app.get('/oauth2/callback', async (req, res) => {
  try {
    const client = oauth2Client();
    const { code } = req.query;
    const { tokens } = await client.getToken(String(code));
    if (!tokens.refresh_token) throw new Error('No refresh_token (ensure prompt=consent)');
    client.setCredentials(tokens);

    const oauth2 = google.oauth2({ version: 'v2', auth: client });
    const me = await oauth2.userinfo.get();
    const email = me.data.email || 'unknown@unknown';
    const name = me.data.name || email;

    const id = uuidv4();
    db.prepare('INSERT INTO accounts (id, email, display_name, refresh_token, created_at) VALUES (?,?,?,?,?)')
      .run(id, email, name, tokens.refresh_token, Date.now());

    res.redirect('/');
  } catch (e) { res.status(500).send(String(e)); }
});

// ---------------- Upload ----------------
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    const accounts = getAccounts();
    if (!accounts.length) return res.status(400).send('Add an account first');

    const tmpPath = req.file.path;
    const suppliedMime = req.file.mimetype || 'application/octet-stream';
    const originalName = safeFileName(req.file.originalname || 'file');
    const chunkMB = Math.max(64, Math.min(2048, Number(req.body.chunk || 512)));

    const fileSha = await sha256File(tmpPath);
    const manifestId = uuidv4();
    const tmpDir = path.join(TMP_CHUNK_DIR, manifestId);
    const { parts, baseName, size } = await splitFileToDir(tmpPath, chunkMB, tmpDir);

    const originalMime = suppliedMime || contentTypeFor(originalName);

    db.prepare('INSERT INTO manifests (id, original_name, original_mime, size, sha256, chunk_size_mb, created_at) VALUES (?,?,?,?,?,?,?)')
      .run(manifestId, originalName, originalMime, size, fileSha, chunkMB, Date.now());

    let aIdx = 0;
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      const acc = accounts[aIdx % accounts.length];
      const { client } = await clientForAccount(acc.id);
      const meta = await driveUpload({ client, name: part.name, mimeType: 'application/octet-stream', data: fssync.createReadStream(part.path) });
      db.prepare('INSERT OR REPLACE INTO parts (manifest_id, idx, size, sha256, account_id, drive_file_id, drive_file_name) VALUES (?,?,?,?,?,?,?)')
        .run(manifestId, part.idx, part.size, part.sha256, acc.id, meta.id, meta.name || part.name);
      aIdx++;
    }

    await fs.rm(tmpPath).catch(()=>{});
    await fs.rm(tmpDir, { recursive: true, force: true }).catch(()=>{});

    res.redirect('/');
  } catch (e) { console.error(e); res.status(500).send(String(e)); }
});

// ---------------- Manifests API ----------------
app.get('/manifests/:id', (req, res) => {
  const id = req.params.id;
  const m = db.prepare('SELECT * FROM manifests WHERE id=?').get(id);
  if (!m) return res.status(404).send('Not found');
  const parts = db.prepare('SELECT * FROM parts WHERE manifest_id=? ORDER BY idx ASC').all(id);
  res.json({ ...m, parts });
});

// ---------------- Download (assembled) ----------------
app.get('/download', async (req, res) => {
  try {
    const manifestId = String(req.query.manifestId);
    const m = db.prepare('SELECT * FROM manifests WHERE id=?').get(manifestId);
    if (!m) return res.status(404).send('Manifest not found');

    const originalName = safeFileName(m.original_name || 'file');
    const ctype = m.original_mime || contentTypeFor(originalName);

    // RFC 5987 filename* for UTF‑8 names
    const dispo = `attachment; filename="${originalName}"; filename*=UTF-8''${encodeURIComponent(originalName)}`;
    res.setHeader('Content-Type', ctype);
    res.setHeader('Content-Disposition', dispo);

    // We don't set Content-Length (streaming from multiple sources)
    const parts = db.prepare('SELECT * FROM parts WHERE manifest_id=? ORDER BY idx ASC').all(manifestId);

    for (const p of parts) {
      const { client } = await clientForAccount(p.account_id);
      const drive = google.drive({ version: 'v3', auth: client });
      const dl = await drive.files.get({ fileId: p.drive_file_id, alt: 'media' }, { responseType: 'stream' });
      await new Promise((resolve, reject) => {
        dl.data.on('error', reject);
        dl.data.on('end', resolve);
        dl.data.pipe(res, { end: false });
      });
    }
    res.end();
  } catch (e) { console.error(e); res.status(500).send(String(e)); }
});

// ---------------- WebDAV (read‑only) ----------------
function davAuth(req, res, next) {
  const creds = basicAuth(req);
  if (!creds || creds.name !== DAV_USER || creds.pass !== DAV_PASS) {
    res.set('WWW-Authenticate', 'Basic realm="SkyPool"');
    return res.status(401).send('Auth required');
  }
  next();
}

function xmlMultiStatus(items) {
  return `<?xml version="1.0" encoding="utf-8"?>\n<d:multistatus xmlns:d="DAV:">${items.join('')}</d:multistatus>`;
}

function xmlResponse(href, isCollection, size) {
  return `\n<d:response>\n  <d:href>${href}</d:href>\n  <d:propstat>\n    <d:prop>\n      <d:resourcetype>${isCollection ? '<d:collection/>' : ''}</d:resourcetype>\n      ${!isCollection ? `<d:getcontentlength>${size}</d:getcontentlength>` : ''}\n    </d:prop>\n    <d:status>HTTP/1.1 200 OK</d:status>\n  </d:propstat>\n</d:response>`;
}

app.options('/dav*', davAuth, (req, res) => {
  res.set({ 'DAV': '1,2', 'Allow': 'OPTIONS, PROPFIND, GET', 'MS-Author-Via': 'DAV' });
  res.status(200).end();
});
app.all('/dav*', davAuth);

app.propfind = app.propfind || function (pathSpec, handler) {
  return app.all(pathSpec, (req, res) => {
    if ((req.method || '').toUpperCase() === 'PROPFIND') handler(req, res);
    else res.status(405).end();
  });
};

app.propfind('/dav', async (req, res) => {
  const depth = req.headers.depth || '1';
  const base = `${BASE_URL}/dav`;
  const manifests = db.prepare('SELECT id, original_name, size FROM manifests ORDER BY created_at DESC').all();
  const items = [ xmlResponse(base + '/', true) ];
  if (depth !== '0') {
    for (const m of manifests) {
      items.push(xmlResponse(`${base}/${encodeURIComponent(m.id)}/`, true));
    }
  }
  res.type('application/xml').status(207).send(xmlMultiStatus(items));
});

app.propfind('/dav/:manifestId', async (req, res) => {
  const id = req.params.manifestId;
  const base = `${BASE_URL}/dav/${encodeURIComponent(id)}`;
  const m = db.prepare('SELECT id, original_name, size FROM manifests WHERE id=?').get(id);
  if (!m) return res.status(404).end();
  const items = [ xmlResponse(base + '/', true) ];
  const fname = safeFileName(m.original_name);
  items.push(xmlResponse(`${base}/${encodeURIComponent(fname)}`, false, m.size));
  res.type('application/xml').status(207).send(xmlMultiStatus(items));
});

app.get('/dav/:manifestId/:filename', async (req, res) => {
  try {
    const { manifestId } = req.params;
    const m = db.prepare('SELECT * FROM manifests WHERE id=?').get(manifestId);
    if (!m) return res.status(404).send('Not found');

    const originalName = safeFileName(m.original_name || req.params.filename);
    const ctype = m.original_mime || contentTypeFor(originalName);
    res.setHeader('Content-Type', ctype);
    res.setHeader('Content-Disposition', `inline; filename="${originalName}"; filename*=UTF-8''${encodeURIComponent(originalName)}`);

    const parts = db.prepare('SELECT * FROM parts WHERE manifest_id=? ORDER BY idx ASC').all(manifestId);
    for (const p of parts) {
      const { client } = await clientForAccount(p.account_id);
      const drive = google.drive({ version: 'v3', auth: client });
      const dl = await drive.files.get({ fileId: p.drive_file_id, alt: 'media' }, { responseType: 'stream' });
      await new Promise((resolve, reject) => { dl.data.on('error', reject); dl.data.on('end', resolve); dl.data.pipe(res, { end: false }); });
    }
    res.end();
  } catch (e) { console.error(e); res.status(500).send(String(e)); }
});

app.listen(PORT, () => console.log(`SkyPool running on ${BASE_URL} — WebDAV at /dav`));
    
