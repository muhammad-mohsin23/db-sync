import { mysqlConnection, pgPool } from "../database/database.service";
import { LogError, LogSuccess } from "../logging/logging";
import {
  insertBooking,
  insertBookingActivity,
  insertBookingAddOns,
  insertBookingFeedback,
  insertBookingServiceDetails,
  insertOneTimeScheduleWindow,
  insertRecurringScheduleItem,
  insertRepeatBookings,
  updateBooking,
  updateBookingAddOn,
  updateBookingFeedback,
  updateBookingServiceDetails,
  updateOneTimeScheduleWindow,
} from "../services/booking";
import {
  deleteCustomerInAccount,
  insertCustomerToAccount,
  updateCustomerInAccount,
} from "../services/customer";
import {
  insertInvoice,
  insertInvoiceItem,
  updateInvoice,
  updateInvoiceItem,
} from "../services/invoice";
import { insertPropertyUnit, insertUnitResident } from "../services/unit";
const EventEmitter = require("node:events");

const eventEmitter = new EventEmitter();

const batchSize = {
  create: 5000,
  update: 25000,
  delete: 25000,
};

export async function fetchData() {
  // const currentDate = new Date().setSeconds(0);
  const toDelete: number[] = [];
  const toSoftDelete: Record<string, string>[] = [];
  const mysqlConn = await mysqlConnection();
  const createBatchSize = batchSize.create;
  try {
    const [rowCount] = (await mysqlConn.execute(`SELECT count(*) as count
      FROM change_log
      WHERE 
   action_type = 'INSERT'
          AND table_name in ('bookings', 'customers','units','unitresidents','repeatbookings','onetimeschedulebookingwindows','recurringschedules')
       AND deleted_at IS NULL;`)) as any;
    const total = rowCount[0]?.count;
    for (let i = 0; i <= total / createBatchSize; i++) {
      const [rows] = await mysqlConn.execute(
        // `SELECT * FROM change_log
        // WHERE created_at >= DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00')  - INTERVAL 3 MINUTE and created_at < DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00')`
        // `SELECT * FROM change_log WHERE deleted_at is null order by Id`
        // `SELECT * FROM change_log
        // WHERE created_at >= ${new Date().toISOString()} - INTERVAL 3 MINUTE and deleted_at is null`

        // `SELECT *
        //     FROM change_log
        //     WHERE
        //     created_at >= '2025-06-12T00:00:00.000Z'
        // AND created_at < '2025-06-13T00:00:00.000Z'
        // AND table_name in ('bookings', 'bookingservicedetails', 'customer' ,'bookingfeedback', 'bookingaddons', 'bookingactivity','invoices','invoicelineitems')
        //      AND deleted_at IS NULL
        //     ORDER BY  limit ${batchSize} offset ${batchSize * i};`

        `SELECT *
            FROM change_log
            WHERE
         table_name in ('bookings', 'customers','units','unitresidents','repeatbookings','onetimeschedulebookingwindows','recurringschedules')
        AND action_type ='INSERT'
             AND deleted_at IS NULL
            ORDER BY created_at limit ${createBatchSize} offset ${
          createBatchSize * i
        };`

        //   `SELECT *
        // FROM change_log
        // WHERE
        // record_id='1414464'
        //  AND deleted_at IS NULL
        // ORDER BY Id limit ${batchSize} offset ${batchSize * i};`
        //       `SELECT *
        // FROM ebdb.change_log
        // WHERE table_name = 'invoices' AND action_type ='UPDATE' AND record_id='1463122'`

        //       `SELECT *
        // FROM ebdb.change_log
        // WHERE table_name = 'bookingservicedetails' AND action_type ='UPDATE' AND record_id='1356041' order by id `
      );
      console.log(`Fetched Rows: `, rows);
      // const [result] = await mysqlConn.execute(
      //   `SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00') AS formatted_time`
      // );
      // console.log("Formatted MySQL time:", result);
      for (const row of rows as any[]) {
        try {
          const newData = row.new_data;
          console.log("New Data:", newData);
          if (
            row.action_type === "INSERT" &&
            row.table_name in insertFunctionsByTablename
          ) {
            await insertFunctionsByTablename[row.table_name]?.(
              newData,
              mysqlConn
            );
          } else {
            throw new Error(
              `Action type ${row.action_type} not supported for table ${row.table_name}`
            );
          }
          toDelete.push(row.Id);
          eventEmitter.emit("log-success", row);
        } catch (error: any) {
          console.error(`Error processing row with ID ${row.Id}:`, error);
          // Optionally, you can push the row to an error array for further investigation
          toSoftDelete.push({ id: row.Id, error: error.message });
          eventEmitter.emit("log-error", { id: row.Id, error: error.message });
        }
      }
    }
  } catch (err) {
    console.error("Error fetching data:", err);
  } finally {
    if (toDelete.length > 0) {
      const mysqlConn = await mysqlConnection();
      try {
        // Delete processed rows from change_log
        await mysqlConn.execute(
          `DELETE FROM change_log WHERE id IN (${toDelete
            .map((x) => `'${x}'`)
            .join()})`
        );

        console.log(`Deleted ${toDelete.length} rows from change_log.`);
      } catch (error) {
        console.error("Error deleting rows from change_log:", error);
      } finally {
        await mysqlConn.end();
      }
    }

    if (toSoftDelete.length > 0) {
      const mysqlConn = await mysqlConnection();
      try {
        // Soft delete processed rows from change_log
        await mysqlConn.execute(
          `UPDATE change_log SET deleted_at = NOW() WHERE id IN (${toSoftDelete
            .map((x) => `'${x.id}'`)
            .join()})`
        );
        console.log(
          `Soft deleted ${
            toSoftDelete.length
          } rows from change_log ${toSoftDelete
            .map((x) => `'${x.id}'`)
            .join()}}.`
        );
      } catch (error) {
        console.error("Error soft deleting rows from change_log:", error);
      } finally {
        console.error(
          "======================= Migration Completed ==========================="
        );
        await mysqlConn.end();
      }
    }
  }
}

export async function fetchUpdateData() {
  // const currentDate = new Date().setSeconds(0);
  const toDelete: number[] = [];
  const toSoftDelete: Record<string, string>[] = [];
  const updateBatchSize = batchSize.update;
  const mysqlConn = await mysqlConnection();
  try {
    const [rowCount] = (await mysqlConn.execute(`SELECT count(*) as count
      FROM change_log
      WHERE 
       action_type ='UPDATED'
          AND table_name in ('bookings', 'bookingservicedetails', 'customers' ,'bookingfeedback', 'bookingaddons', 'bookingactivity','invoices','invoicelineitems')
       AND deleted_at IS NULL;`)) as any;
    const total = rowCount[0]?.count;
    for (let i = 0; i <= total / updateBatchSize; i++) {
      const [rows] = await mysqlConn.execute(
        `SELECT *
            FROM change_log
            WHERE
         table_name in ('bookings', 'bookingservicedetails', 'customers' ,'bookingfeedback', 'bookingaddons', 'bookingactivity','invoices','invoicelineitems')
        AND action_type = 'UPDATE'
             AND deleted_at IS NULL
            ORDER BY created_at limit ${updateBatchSize} offset ${
          updateBatchSize * i
        };`
      );
      console.log(`Fetched Rows: `, rows);

      for (const row of rows as any[]) {
        try {
          const newData = row.new_data;
          console.log("New Data:", newData);
          const toMatchOldData = { ...row.old_data };
          const toMatchNewData = { ...row.new_data };
          delete toMatchOldData["updated_at"];
          delete toMatchOldData["updatedAt"];
          delete toMatchOldData["UpdatedAt"];
          delete toMatchNewData["updated_at"];
          delete toMatchNewData["updatedAt"];
          delete toMatchNewData["UpdatedAt"];
          if (
            JSON.stringify(toMatchOldData) === JSON.stringify(toMatchNewData)
          ) {
            eventEmitter.emit("log-success", {
              ...row,
              message: "nothing to update",
            });
            toDelete.push(row.Id);
            continue;
          }

          if (row.table_name in updateFunctionsByTablename) {
            await updateFunctionsByTablename[row.table_name]?.(
              newData,
              mysqlConn,
              row.Id
            );
          } else {
            throw new Error(
              `Action type ${row.action_type} not supported for table ${row.table_name}`
            );
          }
          toDelete.push(row.Id);
          eventEmitter.emit("log-success", row);
        } catch (error: any) {
          console.error(`Error processing row with ID ${row.Id}:`, error);
          // Optionally, you can push the row to an error array for further investigation
          toSoftDelete.push({ id: row.Id, error: error.message });
          eventEmitter.emit("log-error", { id: row.Id, error: error.message });
        }
      }
      // console.log("Current Date:", new Date(currentDate).toISOString());
      // await mysqlConn.end();
    }
  } catch (err) {
    console.error("Error fetching data:", err);
  } finally {
    if (toDelete.length > 0) {
      const mysqlConn = await mysqlConnection();
      try {
        // Delete processed rows from change_log
        await mysqlConn.execute(
          `DELETE FROM change_log WHERE id IN (${toDelete
            .map((x) => `'${x}'`)
            .join()})`
        );

        console.log(`Deleted ${toDelete.length} rows from change_log.`);
        // LogSuccess(toDelete);
      } catch (error) {
        console.error("Error deleting rows from change_log:", error);
      } finally {
        await mysqlConn.end();
      }
    }

    if (toSoftDelete.length > 0) {
      const mysqlConn = await mysqlConnection();
      try {
        // Soft delete processed rows from change_log
        await mysqlConn.execute(
          `UPDATE change_log SET deleted_at = NOW() WHERE id IN (${toSoftDelete
            .map((x) => `'${x.id}'`)
            .join()})`
        );
        console.log(
          `Soft deleted ${
            toSoftDelete.length
          } rows from change_log ${toSoftDelete
            .map((x) => `'${x.id}'`)
            .join()}}.`
        );
        // LogError(toSoftDelete);
      } catch (error) {
        console.error("Error soft deleting rows from change_log:", error);
      } finally {
        console.error(
          "======================= Migration Completed ==========================="
        );
        await mysqlConn.end();
      }
    }
  }
}

export async function fetchDeleteData() {
  // const currentDate = new Date().setSeconds(0);
  const toDelete: number[] = [];
  const toSoftDelete: Record<string, string>[] = [];
  const mysqlConn = await mysqlConnection();
  const deleteBatchSize = batchSize.delete;

  try {
    const [rowCount] = (await mysqlConn.execute(`SELECT count(*) as count
      FROM change_log
      WHERE 
      table_name in ('bookings', 'bookingservicedetails', 'customer' ,'bookingfeedback', 'bookingaddons', 'bookingactivity','invoices','invoicelineitems')
  AND action_type ='DELETE'
       AND deleted_at IS NULL;`)) as any;
    const total = rowCount[0]?.count;
    for (let i = 0; i <= total / deleteBatchSize; i++) {
      const [rows] = await mysqlConn.execute(
        `SELECT *
            FROM change_log
            WHERE
             action_type ='DELETE'
        AND table_name in ('bookings', 'bookingservicedetails', 'customer' ,'bookingfeedback', 'bookingaddons', 'bookingactivity','invoices','invoicelineitems')
             AND deleted_at IS NULL
            ORDER BY created_at limit ${batchSize} offset ${
          deleteBatchSize * i
        };`
      );
      console.log(`Fetched Rows: `, rows);
      for (const row of rows as any[]) {
        try {
          const newData = row.new_data;
          console.log("New Data:", newData);

          if (row.table_name in deleteFunctionsByTablename) {
            await deleteFunctionsByTablename[row.table_name]?.(
              newData,
              mysqlConn
            );
          } else {
            throw new Error(
              `Action type ${row.action_type} not supported for table ${row.table_name}`
            );
          }
          toDelete.push(row.Id);
          eventEmitter.emit("log-success", row);
        } catch (error: any) {
          console.error(`Error processing row with ID ${row.Id}:`, error);
          // Optionally, you can push the row to an error array for further investigation
          toSoftDelete.push({ id: row.Id, error: error.message });
          eventEmitter.emit("log-error", { id: row.Id, error: error.message });
        }
      }
      // console.log("Current Date:", new Date(currentDate).toISOString());
      // await mysqlConn.end();
    }
  } catch (err) {
    console.error("Error fetching data:", err);
  } finally {
    if (toDelete.length > 0) {
      const mysqlConn = await mysqlConnection();
      try {
        // Delete processed rows from change_log
        await mysqlConn.execute(
          `DELETE FROM change_log WHERE id IN (${toDelete
            .map((x) => `'${x}'`)
            .join()})`
        );

        console.log(`Deleted ${toDelete.length} rows from change_log.`);
        // LogSuccess(toDelete);
      } catch (error) {
        console.error("Error deleting rows from change_log:", error);
      } finally {
        await mysqlConn.end();
      }
    }

    if (toSoftDelete.length > 0) {
      const mysqlConn = await mysqlConnection();
      try {
        // Soft delete processed rows from change_log
        await mysqlConn.execute(
          `UPDATE change_log SET deleted_at = NOW() WHERE id IN (${toSoftDelete
            .map((x) => `'${x.id}'`)
            .join()})`
        );
        console.log(
          `Soft deleted ${
            toSoftDelete.length
          } rows from change_log ${toSoftDelete
            .map((x) => `'${x.id}'`)
            .join()}}.`
        );
        // LogError(toSoftDelete);
      } catch (error) {
        console.error("Error soft deleting rows from change_log:", error);
      } finally {
        console.error(
          "======================= Migration Completed ==========================="
        );
        await mysqlConn.end();
      }
    }
  }
}

const insertFunctionsByTablename: Record<string, Function> = {
  customers: insertCustomerToAccount,
  bookings: insertBooking,
  bookingaddons: insertBookingAddOns,
  bookingfeedback: insertBookingFeedback,
  bookingservicedetails: insertBookingServiceDetails,
  bookingactivity: insertBookingActivity,
  invoices: insertInvoice,
  invoicelineitems: insertInvoiceItem,
  units: insertPropertyUnit,
  unitresidents: insertUnitResident,
  repeatbookings: insertRepeatBookings,
  onetimeschedulebookingwindows: insertOneTimeScheduleWindow,
  recurringschedules: insertRecurringScheduleItem,
};

const updateFunctionsByTablename: Record<string, Function> = {
  customers: updateCustomerInAccount,
  bookings: updateBooking,
  bookingaddons: updateBookingAddOn,
  bookingfeedback: updateBookingFeedback,
  bookingservicedetails: updateBookingServiceDetails,
  invoices: updateInvoice,
  invoicelineitems: updateInvoiceItem,
  onetimeschedulebookingwindows: updateOneTimeScheduleWindow,
};
const deleteFunctionsByTablename: Record<string, Function> = {
  customers: deleteCustomerInAccount,
};

eventEmitter.on("log-success", (data: any) => {
  console.log("started log-success");
  LogSuccess([].concat(data));
});

eventEmitter.on("log-error", (data: any) => {
  console.log("started log-error");
  LogError([].concat(data));
});
