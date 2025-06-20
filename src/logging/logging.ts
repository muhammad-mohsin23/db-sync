// logger.ts
import { createLogger, format, transports } from "winston";
import path, { join, resolve } from "path";
import fs, { appendFile, existsSync, mkdirSync } from "fs";

export function CreateDirectories(directoryPath: string) {
  if (!existsSync(directoryPath)) {
    mkdirSync(directoryPath, { recursive: true });
  }
}

export const ProjectRoot = resolve(__dirname, "../../");

// export const logger = createLogger({
//   level: "info",
//   format: format.combine(
//     format((info) => {
//       // Ensure message is treated as string
//       info.message = String(info.message);
//       return info;
//     })(),
//     format.printf((info) => info.message as string) // Type-safe
//   ),
//   transports: [
//     new transports.File({
//       filename: path.join(logDir, "updated_ids.log"),
//       level: "info",
//     }),

//     // Error-level logs
//     new transports.File({
//       filename: path.join(logDir, "error.log"),
//       level: "error",
//     }),
//   ],
// });

export function LogError(errors: Record<string, string>[]) {
  const timestamp = new Date().toISOString();
  const year = new Date().getFullYear();
  const month = (new Date().getMonth() + 1).toString().padStart(2, "0");
  const day = new Date().getDate().toString().padStart(2, "0");

  const logFolder = join(ProjectRoot, "error-logs", year.toString(), month);
  CreateDirectories(logFolder);

  const logFile = join(logFolder, `${day}.txt`);

  // const logData = `Timestamp: ${timestamp}\nchange_log_id: ${request.method} ${
  //   request.url
  // }\n${request.body ? "Body: " + JSON.stringify(request.body) + "\n" : ""}${
  //   request.user
  //     ? "User: " + `${request.user?.type} ${request.user?.id}` + "\n"
  //     : ""
  // }Error: ${error}\n${log ? `Log: ${JSON.stringify(log)}\n` : ""}\n`;
  let logData = `Timestamp: ${new Date().toISOString()}\n`;
  errors.forEach((error) => {
    logData += `ID: ${error.id}, Error: ${error.error}\n`;
  });
  logData += `\n\n`;

  appendFile(logFile, logData, (err) => {
    if (err) {
      console.error("Error writing to log file:", err);
    }
  });
}

export function LogSuccess(success: Record<string, string>[]) {
  const timestamp = new Date().toISOString();
  const year = new Date().getFullYear();
  const month = (new Date().getMonth() + 1).toString().padStart(2, "0");
  const day = new Date().getDate().toString().padStart(2, "0");

  const logFolder = join(ProjectRoot, "success-logs", year.toString(), month);
  CreateDirectories(logFolder);

  const logFile = join(logFolder, `${day}.txt`);

  // const logData = `Timestamp: ${timestamp}\nchange_log_id: ${request.method} ${
  //   request.url
  // }\n${request.body ? "Body: " + JSON.stringify(request.body) + "\n" : ""}${
  //   request.user
  //     ? "User: " + `${request.user?.type} ${request.user?.id}` + "\n"
  //     : ""
  // }Error: ${error}\n${log ? `Log: ${JSON.stringify(log)}\n` : ""}\n`;
  let logData = `Timestamp: ${new Date().toISOString()}\n`;
  success.forEach((success) => {
    logData += `${JSON.stringify(success)}\n`;
  });
  logData += `\n\n`;

  appendFile(logFile, logData, (err) => {
    if (err) {
      console.error("Error writing to log file:", err);
    }
  });
}
