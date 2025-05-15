import { pgPool } from "../database/database.service";
import { createSlug } from "../helpers/util.herlper";

export async function insertServiceLine(serviceLineData: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    const res = await client.query(
      `SELECT id FROM service_line WHERE legacy_id = $1`,
      [serviceLineData.id]
    );

    if (res.rows.length > 0) {
      await client.query("ROLLBACK");
      return;
    }

    await client.query(
      `INSERT INTO service_line (
        name, description, slug, legacy_id, updated_at
      ) VALUES ($1, $2, $3, $4, $5)`,
      [serviceLineData.name, serviceLineData.name, createSlug(serviceLineData.name), serviceLineData.id, new Date()]
    );

    await client.query("COMMIT");
    console.log(`✅ Service line "${serviceLineData.name}" inserted successfully.`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error inserting service line:", err);
  } finally {
    client.release();
  }
}

export async function insertService(serviceData: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    // Check if service already exists
    const res = await client.query(
      `SELECT id FROM service WHERE legacy_id = $1`,
      [serviceData.id]
    );

    if (res.rows.length > 0) {
      await client.query("ROLLBACK");
      return;
    }

    // Find the mapped service_line_id from the legacy service_line_id
    const servicelineExists = await client.query(
      `SELECT id FROM service_line WHERE legacy_id = $1`,
      [serviceData.id]
    );
    if (servicelineExists.rows.length < 0) {
      await client.query("ROLLBACK");
      console.warn(
        ` No service_line mapping found for ID ${serviceData.service_line_id}`
      );
      return;
    }

    const slug = `${createSlug(serviceData.name)}-${
      serviceData.service_line_id
    }`;
    await client.query(
      `INSERT INTO service (
          name, description, slug, service_line_id, legacy_id, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        serviceData.name,
        serviceData.name,
        slug,
        serviceData.service_line_id,
        serviceData.id,
        new Date(),
      ]
    );

    await client.query("COMMIT");
    console.log(`✅ Service "${serviceData.name}" inserted successfully.`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error inserting service:", err);
  } finally {
    client.release();
  }
}

export async function updateService(serviceData: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    // Check if service already exists
    const res = await client.query(
      `SELECT id FROM service WHERE legacy_id = $1`,
      [serviceData.id]
    );

    if (res.rows.length === 0) {
      await client.query("ROLLBACK");
      return;
    }

    // Find the mapped service_line_id from the legacy service_line_id
    const servicelineExists = await client.query(
      `SELECT id FROM service_line WHERE legacy_id = $1`,
      [serviceData.service_line_id]
    );
    if (servicelineExists.rows.length < 0) {
      await client.query("ROLLBACK");
      console.warn(
        ` No service_line mapping found for ID ${serviceData.service_line_id}`
      );
      return;
    }

    const slug = `${createSlug(serviceData.name)}-${
      serviceData.service_line_id
    }`;
    await client.query(
      `UPDATE service SET
            name = $1,
            description = $2,
            slug = $3,
            service_line_id = $4,
            updated_at = $5
            deleted_at = $6
          WHERE legacy_id = $7`,
      [
        serviceData.name,
        serviceData.name,
        slug,
        serviceData.service_line_id,
        new Date(),
        new Date(),
        serviceData.id,
      ]
    );

    await client.query("COMMIT");
    console.log(`✅ Service "${serviceData.name}" updated successfully.`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error updating service:", err);
  } finally {
    client.release();
  }
}

export async function updateServiceLine(serviceData: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    // Check if service_line with legacy_id exists
    const res = await client.query(
      `SELECT id FROM service_line WHERE legacy_id = $1`,
      [serviceData.id]
    );

    if (res.rows.length === 0) {
      await client.query("ROLLBACK");
      console.warn(` No service line found with legacy_id ${serviceData.id}`);
      return;
    }

    // Proceed to update
    await client.query(
      `UPDATE service_line SET
        name = $1,
        description = $2,
        slug = $3,
        updated_at = $4
        deleted_at = $5
       WHERE legacy_id = $6`,
      [
        serviceData.name,
        serviceData.name,
        createSlug(serviceData.name),
        new Date(),
        new Date(),
        serviceData.id,
      ]
    );

    await client.query("COMMIT");
    console.log(`Service line "${serviceData.name}" updated successfully.`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error(" Error updating service line:", err);
  } finally {
    client.release();
  }
}
