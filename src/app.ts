import express, { Request, Response } from "express";
import { fetchData, fetchUpdateData } from "./cron/cron.service";
import cron from "node-cron";
import { checkPostgresConnection } from "./database/database.service";
import { join } from "path";

const app = express();
const port: number = 3001;

app.use("/error-logs", express.static(join(__dirname, "..", "error-logs")));
app.use("/success-logs", express.static(join(__dirname, "..", "success-logs")));
console.log(join(__dirname, ".."));

app.get("/", async (req: Request, res: Response) => {
  const { action } = req.query;
  if (action === "update") {
    try {
      await fetchUpdateData();
    } catch (error) {
      console.error("Scheduled fetchUpdateData failed:", error);
    }
  } else if (action === "insert") {
    try {
      await fetchData();
    } catch (error) {
      console.error("Scheduled fetchData failed:", error);
    }
  }
  res.send("Hello World!");
});

// app.get("/test", (req: Request, res: Response) => {
//   try {
//     console.log("try");
//     throw new Error();
//   } catch (e) {
//     console.log("catch");
//     throw new Error();
//   } finally {
//     console.log("finally");
//   }
// });

// cron.schedule("0,30 * * * *", async () => {
//   console.log("⏰ Running scheduled fetchData at", new Date().toISOString());
//   try {
//     await fetchData();
//   } catch (error) {
//     console.error("Scheduled fetchData failed:", error);
//   }
// });

cron.schedule("10,40 * * * *", async () => {
  console.log(
    "⏰ Running scheduled fetchUpdateData at",
    new Date().toISOString()
  );
  try {
    await fetchUpdateData();
  } catch (error) {
    console.error("Scheduled fetchUpdateData failed:", error);
  }
});

app.listen(port, async () => {
  try {
    checkPostgresConnection();
    console.log(`Server is running on http://localhost:${port}`);
    console.log("update cron set for every 10 and 40 minute");
  } catch (err) {
    console.error("Error during fetchData execution:", err);
  }
});
