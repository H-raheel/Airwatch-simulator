# Stage 1: Install dependencies
FROM node:20-alpine AS build

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .

# Stage 2: Production runtime
FROM node:20-alpine

WORKDIR /app
COPY --from=build /app .

EXPOSE 3000

CMD ["node", "index.js"]
