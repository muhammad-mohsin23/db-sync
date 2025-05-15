import { pgPool } from "../database/database.service";

export async function insertCustomerToAccount(item: any) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");
    // Insert into account
    const insertAccountRes = await client.query(
      `INSERT INTO account (
first_name, last_name, email, reference_id, username, phone,
company_id, status, zip_code, account_type, legacy_id, updated_at
) VALUES (
$1, $2, $3, $4, $5, $6,
$7, $8, $9, $10, $11, $12) RETURNING id`,
      [
        item.FirstName,
        item.LastName || null,
        item.Email,
        `RN-${item.CustomerId}`,
        item.Email,
        item.MobilePhone,
        null,
        "ACTIVE",
        item.spmZipCode,
        "RESIDENT",
        item.CustomerId,
        new Date(),
      ]
    );
    const accountId = insertAccountRes.rows[0].id;
    // Insert into account_details
    await client.query(
      `INSERT INTO account_details (
account_id, address, city, state, updated_at
) VALUES (
$1, $2, $3, $4, $5
)`,
      [
        accountId,
        item.BillingAddress || null,
        item.City || null,
        item.State || null,
        new Date(),
      ]
    );
    // Insert into account_credential
    const authInfo = JSON.stringify({
      hasher: "bcrypt",
      password: item.Password,
    });
    await client.query(
      `INSERT INTO account_credential (
account_id, provider, provider_key, auth_info,updated_at
) VALUES (
$1, $2, $3, $4, $5
)`,
      [accountId, "PASSWORD", item.Email, authInfo, new Date()]
    );
    await client.query("COMMIT");
    console.log(`Inserted customer ${item.CustomerId}`);
    return accountId;
  } catch (err) {
    await client.query("ROLLBACK");
    console.error(`Error inserting customer ${item.CustomerId}:`, err);
  } finally {
    client.release();
  }
}

export async function updateCustomerInAccount(item: any) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");
    // Check if account exists by legacy_id and email
    const result = await client.query(
      `SELECT id FROM account WHERE legacy_id = $1`,
      [item.CustomerId]
    );
    console.log("Result:", result);
    if (result.rowCount === 0) {
      console.warn(`No existing account found for customer ${item.CustomerId}`);
      await client.query("ROLLBACK");
      return;
    }
    const accountId = result.rows[0].id;
    // Update account
    await client.query(
      `UPDATE account SET
first_name = $1,
last_name = $2,
username = $3,
phone = $4,
zip_code = $5,
status = $6,
account_type = $7,
updated_at = $8
WHERE id = $9`,
      [
        item.FirstName,
        item.LastName || null,
        item.Email,
        item.MobilePhone,
        item.spmZipCode,
        "ACTIVE",
        "RESIDENT",
        new Date(),
        accountId,
      ]
    );
    // Update account_details (optional: add check for existence first if needed)
    await client.query(
      `UPDATE account_details SET
address = $1,
city = $2,
state = $3,
updated_at = $4
WHERE account_id = $5`,
      [
        item.BillingAddress || null,
        item.City || null,
        item.State || null,
        new Date(),
        accountId,
      ]
    );
    // Update account_credential (you can optionally check if credential exists first)
    const authInfo = JSON.stringify({
      hasher: "bcrypt",
      password: item.Password,
    });
    await client.query(
      `UPDATE account_credential SET
auth_info = $1,
updated_at = $2
WHERE account_id = $3 AND provider = 'PASSWORD'`,
      [authInfo, new Date(), accountId]
    );
    await client.query("COMMIT");
    console.log(`Updated customer ${item.CustomerId}`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error(`Error updating customer ${item.CustomerId}:`, err);
  } finally {
    client.release();
  }
}

export async function deleteCustomerInAccount(item: any) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");
    // Check if account exists by legacy_id and email
    const result = await client.query(
      `SELECT id FROM account WHERE legacy_id = $1`,
      [item.CustomerId]
    );
    console.log("Result:", result);
    if (result.rowCount === 0) {
      console.warn(`No existing account found for customer ${item.CustomerId}`);
      await client.query("ROLLBACK");
      return;
    }
    const accountId = result.rows[0].id;
    // Update account
    await client.query(
      `UPDATE account SET
first_name = $1,
last_name = $2,
username = $3,
phone = $4,
zip_code = $5,
status = $6,
account_type = $7,
updated_at = $8
WHERE id = $9`,
      [
        item.FirstName,
        item.LastName || null,
        item.Email,
        item.MobilePhone,
        item.spmZipCode,
        "ACTIVE",
        "RESIDENT",
        new Date(),
        accountId,
      ]
    );
    // Update account_details (optional: add check for existence first if needed)
    await client.query(
      `UPDATE account_details SET
address = $1,
city = $2,
state = $3,
updated_at = $4
WHERE account_id = $5`,
      [
        item.BillingAddress || null,
        item.City || null,
        item.State || null,
        new Date(),
        accountId,
      ]
    );
    // Update account_credential (you can optionally check if credential exists first)
    const authInfo = JSON.stringify({
      hasher: "bcrypt",
      password: item.Password,
    });
    await client.query(
      `UPDATE account_credential SET
auth_info = $1,
updated_at = $2
WHERE account_id = $3 AND provider = 'PASSWORD'`,
      [authInfo, new Date(), accountId]
    );
    await client.query("COMMIT");
    console.log(`Updated customer ${item.CustomerId}`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error(`Error updating customer ${item.CustomerId}:`, err);
  } finally {
    client.release();
  }
}

export async function insertCustomerEntry(item: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    // Step 1: Get the account ID based on legacy customer_id
    const res = await client.query(
      `
      SELECT a.id as account_id, h.id as instruction_id
      FROM account a
      LEFT JOIN home_access_instruction h ON h.account_id = a.id
      WHERE a.legacy_id = $1
      `,
      [item.customer_id]
    );

    const accountId = res.rows[0].account_id;

    if (res.rows[0].instruction_id) {
      await client.query("ROLLBACK");
      console.log(
        `Home access instruction already exists for account_id ${accountId}.`
      );
      return;
    }

    // Step 2: Insert the home access instruction entry
    await client.query(
      `INSERT INTO home_access_instruction (
        entry, entry_code, details, notes, account_id, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        item.method,
        item.code || null,
        item.details || null,
        item.additional_notes || null,
        accountId,
        new Date(),
      ]
    );

    await client.query("COMMIT");
    console.log(`✅ Entry instruction inserted for account_id: ${accountId}`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error inserting entry instruction:", err);
  } finally {
    client.release();
  }
}

export async function getAccountIdByLegacyId(legacyId: number) {
  const client = await pgPool.connect();

  try {
    const res = await client.query(
      `SELECT id FROM account WHERE legacy_id = $1`,
      [legacyId]
    );

    if (res.rows.length === 0) {
      console.log(`ℹ️ Account with legacy_id ${legacyId} not found.`);
      return null;
    }

    return res.rows[0].id;
  } catch (err) {
    console.error("❌ Error fetching account by legacy ID:", err);
    return null;
  } finally {
    client.release();
  }
}
