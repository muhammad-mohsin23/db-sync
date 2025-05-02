import { mysqlConnection } from "../database/database.service";
import {
  deleteCustomerInAccount,
  insertCustomerToAccount,
  updateCustomerInAccount,
} from "../services/customer";
export async function fetchData() {
  const currentDate = new Date().setSeconds(0);
  try {
    const mysqlConn = await mysqlConnection();
    const [rows] = await mysqlConn.execute(
      `SELECT * FROM change_log
WHERE created_at >= DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00') - INTERVAL 3 MINUTE`
    );
    console.log(rows);
    const [result] = await mysqlConn.execute(
      `SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00') AS formatted_time`
    );
    console.log("Formatted MySQL time:", result);
    for (const row of rows as any[]) {
      const newData = row.new_data;
      console.log("New Data:", newData);
      if (
        row.action_type === "INSERT" &&
        row.table_name in insertFunctionsByTablename
      ) {
        await insertFunctionsByTablename[row.table_name]?.(newData);
      } else if (
        row.action_type === "UPDATE" &&
        row.table_name in updateFunctionsByTablename
      ) {
        await updateFunctionsByTablename[row.table_name]?.(newData);
      } else if (
        row.action_type === "DELETE" &&
        row.table_name in deleteFunctionsByTablename
      ) {
        await deleteFunctionsByTablename[row.table_name]?.(newData);
      }
    }
    console.log("Current Date:", new Date(currentDate).toISOString());
    await mysqlConn.end();
  } catch (err) {
    console.error("Error fetching data:", err);
  }
}
const insertFunctionsByTablename: Record<string, Function> = {
  customers: insertCustomerToAccount,
};
const updateFunctionsByTablename: Record<string, Function> = {
  customers: updateCustomerInAccount,
};
const deleteFunctionsByTablename: Record<string, Function> = {
  customers: deleteCustomerInAccount,
};
