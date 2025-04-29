import mysql from "mysql2/promise";
import { Pool } from "pg";
// import dotenv from "dotenv";

// dotenv.config();
// MySQL connection
export const mysqlConnection = async () => {
  try {
    const connection = await mysql.createConnection({
      host: "rds-staging.getspruce.com",
      // user: "srv-bookingeng",
      // password: "kAM7^TrGiGGCGl2IoG9", // Replace with the actual password
      user: "apartmentbutler",
      password: "Click123$", // Replace with the actual password
      database: "ebdb",
      port: 3306,
      connectTimeout: 10000, // Timeout in milliseconds (10 seconds)
    });

    console.log("Connected to MySQL successfully!");
    return connection;
  } catch (error) {
    console.error("Error connecting to MySQL:", error);
    throw error; // Re-throw the error after logging it
  }
};

// PostgreSQL connection
export const pgPool = new Pool({
  host: "spruce-db-prod.cluster-cloi88cqmfxn.us-east-1.rds.amazonaws.com",
  user: "postgres",
  password: "ElgUnf7cv465",
  database: "spruce-live-pms",
  port: 5432,
});

// export const checkPostgresConnection = async () => {
//   try {
//     const client = await pgPool.connect();
//     await client.query("SELECT 1"); // Simple query to check connection
//     client.release();
//     console.log("✅ Connected to PostgreSQL successfully!");
//   } catch (error) {
//     console.error("❌ Error connecting to PostgreSQL:", error);
//     throw error;
//   }
// };
