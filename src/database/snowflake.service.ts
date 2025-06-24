import { createConnection, configure, Connection } from "snowflake-sdk";
// Load the Snowflake Node.js driver.

const snowflakeConn = createConnection({
  account: process.env.SF_ACCOUNT!,
  warehouse: process.env.SF_WAREHOUSE,
  username: process.env.SF_USERNAME,
  password: process.env.SF_PASSWORD,
  database: process.env.SF_DATABASE,
  schema: process.env.SF_SCHEMA,
});

// Configure the Snowflake connection options.
configure({
  logLevel: "INFO",
  logFilePath: "./snowflake_log.log",
  additionalLogToConsole: false,
});

let snowflakeConnectionPromise: Promise<Connection> | null = null;

// let connec: Connection;
const getSnowflakeConnection = (): Promise<Connection> => {
  if (!snowflakeConnectionPromise) {
    snowflakeConnectionPromise = new Promise((resolve, reject) =>
      snowflakeConn.connect(function (err, conn) {
        if (err) {
          console.error("Unable to connect: " + err.message);
          reject(new Error(err.message));
        } else {
          conn
            .isValidAsync()
            .then((isValid) => {
              if (isValid) {
                console.log("Successfully connected to Snowflake.");
                resolve(conn);
              }
              reject(new Error("Connection is not valid."));
            })
            .catch(reject);
          // conn.execute({
          //   sqlText: `SELECT * FROM SPRUCE_WAREHOUSE."PUBLIC".BOOKINGS b limit 10;`,
          //   // streamResult: true,
          //   complete: function (err, stmt, rows) {
          //     if (err) {
          //       console.error(
          //         "Failed to execute statement due to the following error: " +
          //           err.message
          //       );
          //     } else {
          //       console.log("Successfully executed statement: " + stmt.getSqlText());
          //       console.log("🚀 ~ file: app.ts:45 ~ rows:", rows);
          //     }
          //   },
          // });
        }
      })
    );
  }
  return snowflakeConnectionPromise;
};

export async function executeSFQuery(query: string) {
  const sfConn = await getSnowflakeConnection();
  if (!sfConn) {
    throw new Error("Snowflake connection is not established.");
  }
  return await new Promise((res, rej) =>
    sfConn.execute({
      sqlText: query,
      // streamResult: true,
      complete: function (err, stmt, rows) {
        if (err) {
          rej(
            new Error(
              "Failed to execute statement due to the following error: " +
                err.message
            )
          );
        } else {
          console.log("Successfully executed statement: " + stmt.getSqlText());
          res(rows);
        }
      },
    })
  );
}

export async function closeSnowflakeConnection() {
  const conn = await getSnowflakeConnection();
  return new Promise<void>((resolve, reject) => {
    conn.destroy((err) => {
      if (err) {
        console.error("Error closing Snowflake connection:", err.message);
        reject(err);
      } else {
        console.log("Snowflake connection closed.");
        resolve();
      }
    });
  });
}
