import mysql from "mysql2/promise";
import { Pool } from "pg";
import dotenv from "dotenv";

dotenv.config();
// MySQL connection
export const mysqlConnection = async () => {
  try {
    const connection = await mysql.createConnection({
      host: process.env.MYSQL_HOST,
      user: process.env.MYSQL_USER,
      password: process.env.MYSQL_PASSWORD,
      database: process.env.MYSQL_DATABASE,
      port: Number(process.env.MYSQL_PORT),
      connectTimeout: 10000,
    });

    console.log("Connected to MySQL successfully!");
    return connection;
  } catch (error) {
    console.error("Error connecting to MySQL:", error);
    throw error;
  }
};
// PostgreSQL connection
export const pgPool = new Pool({
  host: process.env.PG_HOST,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE,
  port: Number(process.env.PG_PORT),
});

export const checkPostgresConnection = async () => {
  try {
    console.log("Connecting with user:", process.env.PG_USER);
    const client = await pgPool.connect();
    await client.query("SELECT 1"); // Simple query to check connection
    client.release();
    console.log("✅ Connected to PostgreSQL successfully!");
  } catch (error) {
    console.error("❌ Error connecting to PostgreSQL:", error);
    throw error;
  }
};
