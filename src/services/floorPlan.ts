import { pgPool } from "../database/database.service";

export async function insertFloorPlan(item: any, mysqlConn: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    const res = await client.query(
      `SELECT id FROM floor_plan WHERE legacy_id = $1`,
      [item.id]
    );

    if (res.rows.length > 0) {
      await client.query("ROLLBACK");
      throw new Error(`Floor plan with legacy_id ${item.id} already exists.`);
      // return;
    }

    await client.query(
      `INSERT INTO floor_plan (
          name, description, bedrooms, bathrooms, legacy_id, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6)`,
      [item.Name, item.Name, item.Beds, item.Baths, item.id, new Date()]
    );

    await client.query("COMMIT");
    console.log("✅ Floor plan inserted successfully.");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error inserting floor plan:", err);
  } finally {
    client.release();
  }
}

export async function updateFloorPlan(item: any, mysqlConn: any, id: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    const res = await client.query(
      `SELECT id FROM floor_plan WHERE legacy_id = $1`,
      [item.id]
    );

    if (res.rows.length === 0) {
      await client.query("ROLLBACK");
      return;
    }

    await client.query(
      `UPDATE floor_plan SET
            name = $1,
            description = $2,
            bedrooms = $3,
            bathrooms = $4,
            updated_at = $5,
            deleted_at = $6
          WHERE legacy_id = $7`,
      [
        item.Name,
        item.Name,
        item.Beds,
        item.Baths,
        new Date(),
        item.DeletedAt ? new Date(item.DeletedAt) : null,
        item.id,
      ]
    );

    await client.query("COMMIT");
    console.log(
      `✅ Floor plan with legacy_id ${item.id} updated successfully.`
    );
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error updating floor plan:", err);
  } finally {
    client.release();
  }
}
