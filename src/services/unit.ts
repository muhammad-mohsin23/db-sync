import { mysqlConnection, pgPool } from "../database/database.service";

export async function insertPropertyUnit(item: any) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    // Check if property exists
    const propertyRes = await client.query(
      "SELECT id FROM property WHERE legacy_id = $1",
      [item.ApartmentComplexId]
    );
    if (propertyRes.rows?.length === 0) {
      throw new Error(
        `Property with legacy_id ${item.ApartmentComplexId} not found.`
      );
    }
    const propertyId = propertyRes.rows[0].id;

    // Check if floor plan exists
    const floorPlanRes = await client.query(
      "SELECT id FROM floor_plan WHERE legacy_id = $1",
      [item.FloorPlanId]
    );
    if (floorPlanRes.rows?.length === 0) {
      throw new Error(
        `Floor plan with legacy_id ${item.FloorPlanId} not found.`
      );
    }
    const floorPlanId = floorPlanRes.rows[0].id;

    // Insert unit
    await client.query(
      `INSERT INTO unit (
           property_id,
           floor_plan_id,
           number,
           building_number,
           legacy_id,
           updated_at,
           
         ) VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        propertyId,
        floorPlanId,
        item.number,
        item.building_number,
        item.unitID,
        new Date(),
      ]
    );

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("❌ Error upserting unit:", error);
  } finally {
    client.release();
  }
}

export async function insertTenantGroupUnit(item: any) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    // Check if unit exists
    const unitRes = await client.query(
      "SELECT * FROM unit WHERE legacy_id = $1",
      [item.UnitId]
    );

    const unit = unitRes.rows[0];

    if (unitRes.rows?.length === 0) {
      throw new Error(`Unit with legacy_id ${item.UnitId} not found.`);
    }

    const customerAccount = await client.query(
      "SELECT * FROM account WHERE legacy_id = $1",
      [item.CustomerId]
    );

    const property = await client.query(
      "SELECT id FROM property WHERE legacy_id = $1",
      [unit.property_id]
    );

    const tenantGroupRes = await client.query(
      "SELECT id FROM tenant_group WHERE property_id = $1",
      [property.rows[0].id]
    );

    let tenantGroupId;

    if (tenantGroupRes.rows?.length === 0) {
      // 5. If tenant group doesn't exist, create the tenant group
      const tenantGroupInsertRes = await client.query(
        `INSERT INTO tenant_group (property_id, updated_at) 
             VALUES ($1,NOW()) RETURNING id`,
        [item.PropertyId]
      );
      tenantGroupId = tenantGroupInsertRes.rows[0].id;
    } else {
      tenantGroupId = tenantGroupRes.rows[0].id;
    }
    // 2. Check if tenant exists with customerId
    let tenantRes = await client.query(
      `SELECT t.id,t.tenant_group_id
         FROM tenant t
         JOIN account a ON a.tenant_id = t.id
         WHERE a.legacy_id = $1`,
      [item.CustomerId]
    );
    let tenantId;

    if (tenantRes.rows?.length === 0) {
      // Tenant doesn't exist — insert it
      const tenantInsertRes = await client.query(
        `INSERT INTO tenant (first_name, last_name, tenant_group_id, updated_at) 
           VALUES ($1, $2, $3, NOW()) 
           RETURNING id`,
        [
          customerAccount.rows[0].first_name, // corrected property name
          customerAccount.rows[0].last_name,
          tenantGroupId,
        ]
      );
      tenantId = tenantInsertRes.rows[0].id;
    } else {
      tenantId = tenantRes.rows[0].id;

      const existingGroupId = tenantRes.rows[0].tenant_group_id;

      // Update only if tenant_group_id is different or null
      if (existingGroupId !== tenantGroupId) {
        await client.query(
          `UPDATE tenant 
         SET tenant_group_id = $1, updated_at = NOW() 
         WHERE id = $2`,
          [tenantGroupId, tenantId]
        );
      }
    }

    // Insert tenant group unit
    await client.query(
      `INSERT INTO tenant_group_unit (
             tenant_group_id,
             unit_id,
             updated_at
             ) VALUES ($1, $2, $3)`,
      [tenantGroupId, unit?.id, new Date()]
    );

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("❌ Error inserting tenant group unit:", error);
  } finally {
    client.release();
  }
}

export async function getUnitFromUnitResident(unitResidentId: number) {
  const mysqlConn = await mysqlConnection();

  try {
    // Step 1: Get unit_id from MySQL
    const [mysqlRows]: any[] = await mysqlConn.execute(
      `SELECT UnitId FROM unitresidents WHERE Id = ?`,
      [unitResidentId]
    );

    if (!mysqlRows || mysqlRows.length === 0) {
      console.warn(`⚠️ No unitResident found with ID: ${unitResidentId}`);
      return null;
    }

    const legacyUnitId = mysqlRows[0].UnitId;

    // Step 2: Get matching unit from PostgreSQL using legacy_id
    const pgClient = await pgPool.connect();
    const pgRes = await pgClient.query(
      `SELECT id FROM unit WHERE legacy_id = $1`,
      [legacyUnitId]
    );
    pgClient.release();

    if (pgRes.rows.length === 0) {
      console.warn(
        `⚠️ No unit found in PostgreSQL for legacy_id: ${legacyUnitId}`
      );
      return null;
    }

    return pgRes.rows[0].id;
  } catch (err) {
    console.error("❌ Error fetching unit from unitResident:", err);
    return null;
  }
}

// export async function updatePropertyUnit(item: any) {
//   const client = await pgPool.connect();
//   try {
//     await client.query("BEGIN");

//     // Check if property exists
//     const propertyRes = await client.query(
//       "SELECT id FROM property WHERE legacy_id = $1",
//       [item.ApartmentComplexId]
//     );
//     if (propertyRes.rows?.length === 0) {
//       throw new Error(
//         `Property with legacy_id ${item.ApartmentComplexId} not found.`
//       );
//     }
//     const propertyId = propertyRes.rows[0].id;

//     // Check if floor plan exists
//     const floorPlanRes = await client.query(
//       "SELECT id FROM floor_plan WHERE legacy_id = $1",
//       [item.FloorPlanId]
//     );
//     if (floorPlanRes.rows?.length === 0) {
//       throw new Error(
//         `Floor plan with legacy_id ${item.FloorPlanId} not found.`
//       );
//     }
//     const floorPlanId = floorPlanRes.rows[0].id;

//     // Check if unit exists
//     const unitRes = await client.query(
//       "SELECT id FROM unit WHERE legacy_id = $1",
//       [item.unitID]
//     );
//     if (unitRes.rows?.length === 0) {
//       throw new Error(`Unit with legacy_id ${item.unitID} not found.`);
//     }

//     // Update unit
//     await client.query(
//       `UPDATE unit SET
//          property_id = $1,
//          floor_plan_id = $2,
//          number = $3,
//          building_number = $4,
//          updated_at = $5
//        WHERE legacy_id = $6`,
//       [
//         propertyId,
//         floorPlanId,
//         item.number,
//         item.building_number,
//         new Date(),
//         item.unitID,
//       ]
//     );

//     await client.query("COMMIT");
//     console.log(`✅ Unit "${item.number}" updated successfully.`);
//   } catch (error) {
//     await client.query("ROLLBACK");
//     console.error("❌ Error updating unit:", error);
//   } finally {
//     client.release();
//   }
// }
