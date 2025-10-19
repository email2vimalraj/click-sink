# Build UI
FROM node:20-slim AS builder
WORKDIR /app
COPY web/nextjs/package*.json ./
RUN npm ci
COPY web/nextjs .
RUN npm run build

# Run UI
FROM node:20-slim
WORKDIR /app
ENV NODE_ENV=production
COPY --from=builder /app .
EXPOSE 3000
CMD ["npm", "start"]
