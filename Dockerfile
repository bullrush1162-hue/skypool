FROM node:20-slim
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production || npm i --omit=dev
COPY server.js ./
ENV NODE_ENV=production
EXPOSE 8080
CMD ["node", "server.js"]
