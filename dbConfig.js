const mysql = require('mysql2/promise');
const { Client } = require('pg');
const {
  MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE,
  PG_HOST, PG_USER, PG_PASSWORD, PG_DATABASE
} = process.env;

async function ensureMySQLDatabaseExists() {
  const connection = await mysql.createConnection({
    host: MYSQL_HOST,
    user: MYSQL_USER,
    password: MYSQL_PASSWORD
  });

  await connection.query(`CREATE DATABASE IF NOT EXISTS \`${MYSQL_DATABASE}\`;`);
  await connection.query(`
    CREATE TABLE IF NOT EXISTS \`${MYSQL_DATABASE}\`.air_sensors (
      id INT AUTO_INCREMENT PRIMARY KEY,
      sensor_id VARCHAR(50),
      timestamp DATETIME,
      pm25 FLOAT,
      pm10 FLOAT,
      co FLOAT,
      no2 FLOAT,
      o3 FLOAT,
      so2 FLOAT,
      temperature FLOAT,
      humidity FLOAT
    );
  `);
  await connection.end();
}

async function ensurePostgresDatabaseExists() {
  const adminClient = new Client({
    host: PG_HOST,
    user: PG_USER,
    password: PG_PASSWORD,
    database: 'postgres',
    ssl: { rejectUnauthorized: false }
  });

  await adminClient.connect();
  const result = await adminClient.query(`SELECT 1 FROM pg_database WHERE datname = $1`, [PG_DATABASE]);

  if (result.rowCount === 0) {
    await adminClient.query(`CREATE DATABASE ${PG_DATABASE}`);
    console.log(`Created PostgreSQL database: ${PG_DATABASE}`);
  }

  await adminClient.end();

  const pgClient = new Client({
    host: PG_HOST,
    user: PG_USER,
    password: PG_PASSWORD,
    database: PG_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  await pgClient.connect();
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS air_sensors (
      id SERIAL PRIMARY KEY,
      sensor_id VARCHAR(50),
      timestamp TIMESTAMP,
      pm25 FLOAT,
      pm10 FLOAT,
      co FLOAT,
      no2 FLOAT,
      o3 FLOAT,
      so2 FLOAT,
      temperature FLOAT,
      humidity FLOAT
    );
  `);
  await pgClient.end();
}

async function initMySQLPool() {
  return mysql.createPool({
    host: MYSQL_HOST,
    user: MYSQL_USER,
    password: MYSQL_PASSWORD,
    database: MYSQL_DATABASE,
    waitForConnections: true,
    connectionLimit: 10
  });
}

async function initPostgresClient() {
  const pgClient = new Client({
    host: PG_HOST,
    user: PG_USER,
    password: PG_PASSWORD,
    database: PG_DATABASE,
    ssl: { rejectUnauthorized: false }
  });
  await pgClient.connect();
  return pgClient;
}

module.exports = {
  ensureMySQLDatabaseExists,
  ensurePostgresDatabaseExists,
  initMySQLPool,
  initPostgresClient,
  ensureDatabases: async () => {
    await ensureMySQLDatabaseExists();
    await ensurePostgresDatabaseExists();
  }
};
