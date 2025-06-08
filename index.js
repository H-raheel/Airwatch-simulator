require('dotenv').config();
const mysql = require('mysql2/promise');
const { Client } = require('pg');
const { faker } = require('@faker-js/faker');


const {
  MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE,
  PG_HOST, PG_USER, PG_PASSWORD, PG_DATABASE,
  DATA_INTERVAL_SECONDS
} = process.env;

async function main() {
  // Connect MySQL
  const mysqlConnection = await mysql.createPool({
    host: MYSQL_HOST,
    user: MYSQL_USER,
    password: MYSQL_PASSWORD,
    database: MYSQL_DATABASE,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
  });

  // Connect PostgreSQL
  const pgClient = new Client({
    host: PG_HOST,
    user: PG_USER,
    password: PG_PASSWORD,
    database: PG_DATABASE,
  });
  await pgClient.connect();

  // Periodically insert fake air sensor data into MySQL
  setInterval(async () => {
    try {
      const sensorData = generateFakeSensorData();
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
        sensorData.humidity,
      ];

      await mysqlConnection.execute(sql, values);
      console.log('Inserted new sensor data:', sensorData);
    } catch (error) {
      console.error('MySQL insert error:', error);
    }
  }, (DATA_INTERVAL_SECONDS || 10) * 1000);

  // Periodically sync latest data from MySQL to PostgreSQL
  setInterval(async () => {
    try {
      // Fetch recent data from MySQL (e.g., last 100 rows)
      const [rows] = await mysqlConnection.query(
        `SELECT * FROM air_sensors ORDER BY timestamp DESC LIMIT 100`
      );

      // Upsert into PostgreSQL table air_sensors (make sure table exists)
      for (const row of rows) {
        const upsertQuery = `
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
            humidity = EXCLUDED.humidity;`;

        const pgValues = [
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

        await pgClient.query(upsertQuery, pgValues);
      }

      console.log('Synced latest data from MySQL to PostgreSQL');
    } catch (error) {
      console.error('Postgres sync error:', error);
    }
  }, 60000); // every 60 seconds
}

function generateFakeSensorData() {
  return {
    sensor_id: 'sensor-' + faker.datatype.number({ min: 1, max: 10 }),
    timestamp: new Date(),
    pm25: faker.datatype.float({ min: 0, max: 500, precision: 0.1 }),
    pm10: faker.datatype.float({ min: 0, max: 500, precision: 0.1 }),
    co: faker.datatype.float({ min: 0, max: 50, precision: 0.01 }),
    no2: faker.datatype.float({ min: 0, max: 200, precision: 0.01 }),
    o3: faker.datatype.float({ min: 0, max: 300, precision: 0.01 }),
    so2: faker.datatype.float({ min: 0, max: 100, precision: 0.01 }),
    temperature: faker.datatype.float({ min: -10, max: 45, precision: 0.1 }),
    humidity: faker.datatype.float({ min: 0, max: 100, precision: 0.1 }),
  };
}

main().catch(console.error);
