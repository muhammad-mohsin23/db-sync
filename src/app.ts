import express, { Request, Response } from "express";
import { fetchData } from "./cron/cron.service";
import cron from "node-cron";
import { checkPostgresConnection } from "./database/database.service";

const app = express();
const port: number = 3001;

app.get("/", (req: Request, res: Response) => {
  res.send("Hello World!");
});

cron.schedule("*/3 * * * *", async () => {
  console.log("⏰ Running scheduled fetchData at", new Date().toISOString());
  try {
    await fetchData();
  } catch (error) {
    console.error("Scheduled fetchData failed:", error);
  }
});

app.listen(port, async () => {
  try {
    checkPostgresConnection();
    console.log(`Server is running on http://localhost:${port}`);
  } catch (err) {
    console.error("Error during fetchData execution:", err);
  }
});
