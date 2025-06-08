require('dotenv').config();
const { faker } = require('@faker-js/faker');
const express = require('express');
const { initMySQLPool, initPostgresClient, ensureDatabases } = require('./dbConfig');

const {
  DATA_INTERVAL_SECONDS,
  PORT = 3000
} = process.env;

let mysqlPool;
let pgClient;
let lastInsertedData = null;

async function main() {
  // Ensure databases and tables
  await ensureDatabases();

  // Initialize connections
  mysqlPool = await initMySQLPool();
  pgClient = await initPostgresClient();

  // Periodic fake data insertion into MySQL
  setInterval(async () => {
    try {
      const sensorData = generateFakeSensorData();
      lastInsertedData = sensorData;

      const sql = `
        INSERT INTO air_sensors 
        (sensor_id, timestamp, pm25, pm10, co, no2, o3, so2, temperature, humidity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

      const values = [
        sensorData.sensor_id,
        sensorData.timestamp,
        sensorData.pm25,
        sensorData.pm10,
        sensorData.co,
        sensorData.no2,
        sensorData.o3,
        sensorData.so2,
        sensorData.temperature,
        sensorData.humidity
      ];

      await mysqlPool.execute(sql, values);
      console.log('Inserted new sensor data:', sensorData);
    } catch (err) {
      console.error('MySQL Insert Error:', err.message);
    }
  }, (DATA_INTERVAL_SECONDS || 10) * 1000);

  // Periodic sync from MySQL to PostgreSQL
  setInterval(async () => {
    try {
      const [rows] = await mysqlPool.query(
        `SELECT * FROM air_sensors ORDER BY timestamp DESC LIMIT 100`
      );

      for (const row of rows) {
        const upsert = `
          INSERT INTO air_sensors (id, sensor_id, timestamp, pm25, pm10, co, no2, o3, so2, temperature, humidity)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
          ON CONFLICT (id) DO UPDATE SET
            sensor_id = EXCLUDED.sensor_id,
            timestamp = EXCLUDED.timestamp,
            pm25 = EXCLUDED.pm25,
            pm10 = EXCLUDED.pm10,
            co = EXCLUDED.co,
            no2 = EXCLUDED.no2,
            o3 = EXCLUDED.o3,
            so2 = EXCLUDED.so2,
            temperature = EXCLUDED.temperature,
            humidity = EXCLUDED.humidity`;

        const values = [
          row.id,
          row.sensor_id,
          row.timestamp,
          row.pm25,
          row.pm10,
          row.co,
          row.no2,
          row.o3,
          row.so2,
          row.temperature,
          row.humidity
        ];

        await pgClient.query(upsert, values);
      }

      console.log('Synced latest data to PostgreSQL');
    } catch (err) {
      console.error('PostgreSQL Sync Error:', err.message);
    }
  }, 60000);
}

function generateFakeSensorData() {
  return {
    sensor_id: 'sensor-' + faker.number.int({ min: 1, max: 10 }),
    timestamp: new Date(),
    pm25: faker.number.float({ min: 0, max: 500, precision: 0.1 }),
    pm10: faker.number.float({ min: 0, max: 500, precision: 0.1 }),
    co: faker.number.float({ min: 0, max: 50, precision: 0.01 }),
    no2: faker.number.float({ min: 0, max: 200, precision: 0.01 }),
    o3: faker.number.float({ min: 0, max: 300, precision: 0.01 }),
    so2: faker.number.float({ min: 0, max: 100, precision: 0.01 }),
    temperature: faker.number.float({ min: -10, max: 45, precision: 0.1 }),
    humidity: faker.number.float({ min: 0, max: 100, precision: 0.1 })
  };
}

// Express HTTP server
const app = express();

app.get('/', (req, res) => {
  res.send('Airwatch service is healthy');
});

app.get('/latest-data', (req, res) => {
  if (lastInsertedData) res.json(lastInsertedData);
  else res.status(204).send('No data generated yet');
});

app.listen(PORT, () => {
  console.log(`HTTP server listening on port ${PORT}`);
});

main().catch(err => console.error('Startup error:', err));
