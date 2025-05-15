import { daysOfWeek } from "../constants/constant";
import { pgPool } from "../database/database.service";
import { getAccountIdByLegacyId } from "./customer";
import { getRegionByLegacyId } from "./region";

export async function getPropertyIdByLegacyId(legacyId: number) {
  const client = await pgPool.connect();

  try {
    const res = await client.query(
      `SELECT id FROM property WHERE legacy_id = $1`,
      [legacyId]
    );

    if (res.rows.length === 0) {
      console.log(`ℹ️ Property with legacy_id ${legacyId} not found.`);
      return null;
    }

    return res.rows[0].id;
  } catch (err) {
    console.error("❌ Error fetching property by legacy ID:", err);
    return null;
  } finally {
    client.release();
  }
}

export async function insertProperty(propertyData: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    // Check if property already exists by legacy_id
    const exists = await client.query(
      `SELECT id FROM property WHERE legacy_id = $1`,
      [propertyData.ApartmentId]
    );

    if (exists.rows.length > 0) {
      await client.query("ROLLBACK");
      console.log(
        `Property with legacy_id ${propertyData.ApartmentId} already exists.`
      );
      return exists.rows[0].id;
    }

    // Region and Manager lookup helpers
    const regionId = await getRegionByLegacyId(propertyData.Region); // implement this

    let companyId: number | null = null;
    if (propertyData.ManagementCompanyId) {
      const companyResult = await client.query(
        `SELECT id FROM company WHERE legacy_id = $1`,
        [propertyData.ApartmentManagerId]
      );

      if (companyResult.rows.length > 0) {
        companyId = companyResult.rows[0].id;
      } else {
        const companyInsert = await client.query(
          `INSERT INTO company (
            type,
            name,
            phone_number,
            email,
            website,
            status,
            legacy_id,
            updated_at
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
          RETURNING id`,
          [
            "PROPERTY_MANAGER",
            propertyData.ManagementCompany ?? null,
            null,
            null,
            null,
            "ACTIVE",
            propertyData.ManagementCompanyId,
            new Date(),
          ]
        );
        companyId = companyInsert.rows[0].id;
      }
    }

    let propertyNote = null;

    if (propertyData.HKNotes?.trim()) {
      propertyNote = `HK Notes: ${propertyData.HKNotes}\n`;
    }

    if (propertyData.DCNotes?.trim()) {
      propertyNote = `DC Notes: ${propertyData.DCNotes}\n`;
    }

    if (propertyData.DWNotes?.trim()) {
      propertyNote = `DW Notes: ${propertyData.DWNotes}\n`;
    }
    const now = new Date();
    const result = await client.query(
      `INSERT INTO property (
        name,
        type,
        website,
        hidden,
        do_not_call,
        region_id,
        time_zone,
        property_note,
        legacy_id,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      RETURNING id`,
      [
        // propertyManagerId, add the company id here
        propertyData.ApartmentComplexName,
        propertyData.Type ?? null,
        propertyData.Website ?? null,
        propertyData.Visible === 1 ? false : true,
        propertyData.DoNotCall ?? false,
        regionId,
        propertyData.Timezone ?? null,
        propertyNote, // property_note placeholder, you can extract from notes if needed
        propertyData.ApartmentId,
        now,
      ]
    );
    const propertyId = result.rows[0].id;

    // --- EMAIL INSERT ---
    if (propertyData.Email && propertyId) {
      try {
        await client.query(
          `INSERT INTO property_email (
            property_id,
            email_address,
            is_primary,
            updated_at
          ) VALUES ($1, $2, $3, $4)`,
          [propertyId, propertyData.EMAIL, true, now]
        );
      } catch (err) {
        console.warn("Duplicate email:", propertyData.Email);
      }
    }

    // --- PHONE INSERT ---
    if (propertyData.PhoneNumber && propertyId) {
      try {
        await client.query(
          `INSERT INTO property_phone (
        property_id,
        phone_number,
        type,
        name,
        is_primary,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6)`,
          [
            propertyId,
            propertyData.PhoneNumber ?? null,
            "-", // default phone type
            propertyData.ApartmentComplexName ?? null,
            true,
            now,
          ]
        );
      } catch (err) {
        console.warn("Duplicate phone:", propertyData.acPhoneNumber);
      }
    }

    // --- ADDRESS INSERT ---
    if (propertyData.Address && propertyId) {
      try {
        await client.query(
          `INSERT INTO property_address (
        property_id,
        street1,
        city,
        state,
        postal_code,
        county,
        country,
        region,
        type,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
          [
            propertyId,
            propertyData.Address ?? null,
            propertyData.City ?? null,
            propertyData.State ?? null,
            propertyData.ZipCode ?? null,
            null, // county is null as in original
            "United States",
            propertyData.Region ?? null,
            "Physical",
            now,
          ]
        );
      } catch (err) {
        console.warn("Duplicate address for property:", propertyId);
      }
    }

    // Days of the week mapping
    for (const [dayName, dayIndex] of Object.entries(daysOfWeek)) {
      const openKey = `${dayName}Open`;
      const closeKey = `${dayName}Close`;

      const openTime = propertyData[openKey];
      const closeTime = propertyData[closeKey];

      if (openTime && closeTime) {
        await client.query(
          `INSERT INTO property_hours (
            property_id,
            day_of_week,
            begin,
            end,
            updated_at
          ) VALUES ($1, $2, $3, $4, $5)`,
          [propertyId, dayIndex, openTime, closeTime, now]
        );
      }
    }

    if (companyId && propertyId) {
      await client.query(
        `INSERT INTO company_property (
          property_id,
          company_id,
          updated_at
        ) VALUES ($1, $2, $3)`,
        [propertyId, companyId, now, now]
      );
    }

    await client.query("COMMIT");
    console.log(`✅ Property inserted with ID: ${result.rows[0].id}`);
    return result.rows[0].id;
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error inserting property:", err);
  } finally {
    client.release();
  }
}

export async function insertPropertyManager(propertyManagerData: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    const legacyId = propertyManagerData.Id;

    // Check if account already exists
    const existing = await client.query(
      `SELECT id FROM account WHERE legacy_id = $1`,
      [legacyId]
    );

    let accountId: number;

    if (existing.rows.length === 0) {
      const now = new Date();

      // Insert into account
      const res = await client.query(
        `INSERT INTO account (
          first_name,
          last_name,
          email,
          username,
          status,
          account_type,
          teammate,
          legacy_id,
          updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7,$8, $9)
        RETURNING id`,
        [
          propertyManagerData.FirstName,
          propertyManagerData.LastName,
          propertyManagerData.email,
          propertyManagerData.email ?? propertyManagerData.FirstName,
          "ACTIVE",
          "PROPERTY_MANAGER",
          "true",
          legacyId,
          now,
        ]
      );

      accountId = res.rows[0].id;
      console.log(`✅ Created Account ID: ${accountId}`);

      // Insert account_details
      await client.query(
        `INSERT INTO account_details (
          account_id,
          preferredLanguage,
          updated_at
        ) VALUES ($1, $2, $3)`,
        [accountId, "ENGLISH", now]
      );

      // Insert account_credential
      const authInfo = JSON.stringify({
        hasher: "bcrypt",
        password:
          "$2a$10$Fl2kV/HXu4lq/GKcWMLbs.WEeElv8H7NveUaot3BjozH1sscrJH06",
      });

      await client.query(
        `INSERT INTO account_credential (
          account_id,
          provider,
          provider_key,
          auth_info,
          updated_at
        ) VALUES ($1, 'PASSWORD', $2, $3, $4, $5)`,
        [accountId, propertyManagerData.email, authInfo, now]
      );
    } else {
      accountId = existing.rows[0].id;
      console.log(`ℹ️ Account already exists. Updating ID: ${accountId}`);
    }

    await client.query("COMMIT");
    return accountId;
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error in insertPropertyManager:", err);
    return null;
  } finally {
    client.release();
  }
}

export async function connectPropertyManagerToProperty(
  apartmentComplexData: any
) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    // Check if the connection already exists
    const existing = await client.query(
      `SELECT id FROM property WHERE legacy_id = $1`,
      [apartmentComplexData.ApartmentId]
    );

    if (existing.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(
        `No property found with legacy_id ${apartmentComplexData.ApartmentId}`
      );
      return;
    }
    const propertyId = existing.rows[0].id;

    const propertyManager = await client.query(
      `SELECT id FROM account WHERE legacy_id = $1`,
      [apartmentComplexData.UserId]
    );

    const propertyManagerId = propertyManager.rows[0].id;

    if (!propertyManagerId) {
      await client.query("ROLLBACK");
      console.log(
        `No property manager found with legacy_id ${apartmentComplexData.UserId}`
      );
      return;
    }
    // Insert into company_property
    await client.query(
      `UPDATE property
       SET property_manager_id = $1,
           updated_at = $2
       WHERE id = $3`,
      [propertyManagerId, new Date(), propertyId]
    );

    await client.query("COMMIT");
    console.log(
      `✅ Connected Property ID ${apartmentComplexData.ApartmentId} with Manager ID `
    );
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error connecting property manager to property:", err);
  } finally {
    client.release();
  }
}
