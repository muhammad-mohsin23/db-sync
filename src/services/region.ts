import { pgPool } from "../database/database.service";

export async function insertMarket(item: any, mysqlConn: any) {
  const pgConn = await pgPool.connect();

  try {
    const result = await pgConn.query(
      `SELECT id FROM region WHERE legacy_id = $1`,
      [item.MarketId]
    );

    if (result.rows.length > 0) {
      return;
    }

    await pgConn.query(
      `INSERT INTO region (name, description, short_name, time_zone, legacy_id, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, NOW(), NOW())`,
      [
        item.City,
        item.ShortName,
        item.ShortName,
        item.Timezone || null,
        item.MarketId,
      ]
    );

    console.log("✅ Markets insertion completed.");
  } catch (err) {
    console.error("❌ Error inserting markets:", err);
  } finally {
    pgConn.release();
  }
}

export async function updateMarket(item: any, mysqlConn: any, id?: any) {
  const pgConn = await pgPool.connect();

  try {
    // Insert new market
    await pgConn.query(
      `UPDATE region
        SET name = $1,
            description = $2,
            short_name = $3,
            time_zone = $4,
            deleted_at =$5,
            updated_at = NOW()
        WHERE legacy_id = $6`,
      [
        item.City,
        item.ShortName, // Use City for both name and description
        item.ShortName,
        item.Timezone ? item.Timezone : null,
        item.DeletedAt ? new Date(item.DeletedAt) : null,
        item.MarketId,
      ]
    );

    console.log("✅ Market updated successfully.");
  } catch (err) {
    console.error("❌ Error updating market:", err);
  } finally {
    pgConn.release();
  }
}

export async function getRegionByLegacyId(legacyId: number) {
  const client = await pgPool.connect();

  try {
    const res = await client.query(
      `SELECT id FROM region WHERE legacy_id = $1`,
      [legacyId]
    );

    if (res.rows.length === 0) {
      console.log(`ℹ️ Region with legacy_id ${legacyId} not found.`);
      return null;
    }

    return res.rows[0].id;
  } catch (err) {
    console.error("❌ Error fetching region by legacy ID:", err);
    return null;
  } finally {
    client.release();
  }
}
