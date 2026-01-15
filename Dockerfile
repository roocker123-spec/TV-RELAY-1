FROM node:18-slim

WORKDIR /app

# Install deps first (better caching)
COPY package*.json ./
RUN npm ci --omit=dev

# Copy app
COPY . .

# Cloud Run provides PORT automatically
ENV NODE_ENV=production

CMD ["npm","start"]
