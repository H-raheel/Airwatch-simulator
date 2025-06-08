# Stage 1: Build
FROM node:20-alpine AS build

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build  # optional if you have a build step, else skip

# Stage 2: Production runtime
FROM node:20-alpine

WORKDIR /app
COPY --from=build /app ./

EXPOSE 3000

CMD ["node", "index.js"]
