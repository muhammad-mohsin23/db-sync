import { pgPool } from "../database/database.service";

export async function createNewServiceProvider() {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    // Insert company
    const companyResult = await client.query(
      `INSERT INTO company (
        type, name, phone_number, email, website, status, legacy_id, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9
      ) RETURNING id`,
      [
        "SERVICE_PROVIDER",
        "Jodec Cleaners",
        null,
        null,
        null,
        "ACTIVE",
        null,
        new Date(),
        new Date(),
      ]
    );
    const companyId = companyResult.rows[0].id;

    // Users list
    const users = [
      {
        name: "Betty Olweny",
        email: "betty@jodeccleaners.com",
        phone: "571-778-4216",
      },
      {
        name: "Jorge Garcia",
        email: "jorge_luis_garcia_lacherre@outlook.com",
        phone: "703-346-3838",
      },
    ];

    for (const user of users) {
      // Insert account
      const accountResult = await client.query(
        `INSERT INTO account (
          first_name, last_name, email, username, phone,
          company_id, status, account_type, legacy_id, created_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7, $8, $9, $10, $11
        ) RETURNING id`,
        [
          user.name,
          user.name,
          user.email,
          user.email,
          user.phone,
          companyId,
          "ACTIVE",
          "SERVICE_PROVIDER",
          null,
          new Date(),
          new Date(),
        ]
      );
      const accountId = accountResult.rows[0].id;

      // Insert account_details
      await client.query(
        `INSERT INTO account_details (
          account_id, preferredLanguage, created_at, updated_at
        ) VALUES ($1, $2, $3, $4)`,
        [accountId, "ENGLISH", new Date(), new Date()]
      );

      // Insert account_credential
      const passwordFormat = JSON.stringify({
        hasher: "bcrypt",
        password:
          "$2a$10$Fl2kV/HXu4lq/GKcWMLbs.WEeElv8H7NveUaot3BjozH1sscrJH06",
      });

      await client.query(
        `INSERT INTO account_credential (
          account_id, provider, provider_key, auth_info, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          accountId,
          "PASSWORD",
          user.email,
          passwordFormat,
          new Date(),
          new Date(),
        ]
      );
    }

    await client.query("COMMIT");
    console.log("✅ Service provider and users created successfully.");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error creating service provider:", err);
  } finally {
    client.release();
  }
}
