import pgPool from "../database/database.service";
import { executeSFQuery } from "../database/snowflake.service";

export async function syncBraintreeTransactions(whereClause: string) {
  const sqlText = `SELECT * FROM SPRUCE_WAREHOUSE."PUBLIC".BRAINTREE_TRANSACTIONS ${whereClause}`;
  const rows = (await executeSFQuery(sqlText)) as {
    BRAINTREE_TRANSACTION_ID: string;
    BOOKING_ID: string;
    DATE: Date;
  }[];

  let values = rows.reduce(
    (acc, cur) =>
      (acc += `(${
        cur.BRAINTREE_TRANSACTION_ID
      },'${cur.DATE?.toISOString()}'),`),
    ""
  );
  values = values.substring(0, values.length - 1);
  const pgQuery = `UPDATE invoice AS i
SET "brain_tree_date" = v.brain_tree_date
FROM (
    VALUES
        ${values}
) AS v(brain_tree_id, brain_tree_date)
WHERE i.brain_tree_id = v.brain_tree_id and i.brain_tree_date IS NULL;
 `;
  const client = await pgPool.connect();
  const result = await client.query(pgQuery);
  console.log(
    `Updated ${result.rowCount} invoices with Braintree transactions.`
  );
  return;
}
