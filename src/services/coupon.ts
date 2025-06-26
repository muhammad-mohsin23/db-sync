import { AdminId } from "../constants/constant";
import { pgPool } from "../database/database.service";
import { getCustomerAccountIdByLegacyId } from "./customer";

export async function insertCoupon(couponData: any, mysqlConn: any) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    // Insert coupon
    const isPublic = couponData.Visible === 1;
    const expiryDate = new Date(couponData.Expiration);
    expiryDate.setUTCHours(23, 59, 59); // Ensures time is end of day

    const result = await client.query(
      `INSERT INTO coupon (
            code, amount, expiry_date, is_public, legacy_id, updated_at
          ) VALUES (
            $1, $2, $3, $4, $5, $6
          )
          RETURNING id`,
      [
        couponData.CouponCode,
        couponData.DollarAmount,
        expiryDate,
        isPublic,
        couponData.CouponId,
        new Date(),
      ]
    );

    const couponId = result.rows[0].id;

    // Try to fetch customer account after inserting coupon
    const customerAccountId = await getCustomerAccountIdByLegacyId(
      couponData.CustomerId
    );

    if (customerAccountId && couponId) {
      // Create entry in user_coupons only if account exists
      await client.query(
        `INSERT INTO user_coupons (account_id, coupon_id, updated_at)
           VALUES ($1, $2, NOW())`,
        [customerAccountId, couponId]
      );
    }

    await client.query("COMMIT");
    console.log(`Inserted coupon ${couponData.CouponCode}`);
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error inserting coupon:", error);
  } finally {
    client.release();
  }
}

export async function insertCreditsToCoupon(couponData: any) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    const exists = await couponExists(couponData.Id, "CREDIT");
    if (exists) throw new Error("credit already exists");

    const couponCode = `CREDIT-${couponData.Id}`;
    if (couponData.BookingId) {
      const bookingRes = await client.query(
        `select id from booking where legacy_id = $1`,
        [couponData.BookingId]
      );
      couponData.BookingId = bookingRes.rows[0]?.id;
    }

    // Insert credit coupon
    const result = await client.query(
      `INSERT INTO coupon (
            code, description, reason, amount, coupon_type, category, legacy_id, active_date, booking_id, created_by_id, created_at, updated_at
          ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
          )
          RETURNING id`,
      [
        couponCode,
        couponData.Notes,
        couponData.Notes,
        couponData.Amount,
        "CREDIT",
        "SUPPORT",
        couponData.Id,
        couponData.CreatedAt,
        couponData.BookingId,
        AdminId,
        couponData.CreatedAt,
        couponData.UpdatedAt ?? couponData.CreatedAt,
      ]
    );

    const couponId = result.rows[0].id;

    // Try to fetch customer account after inserting coupon
    const customerAccountId = await getCustomerAccountIdByLegacyId(
      couponData.CustomerId
    );

    if (customerAccountId && couponId) {
      // Create entry in user_coupons only if account exists
      await client.query(
        `INSERT INTO user_coupons (account_id, coupon_id, updated_at)
           VALUES ($1, $2, NOW())`,
        [customerAccountId, couponId]
      );
    }

    await client.query("COMMIT");
    console.log(`Inserted credit coupon ${couponData.CouponCode}`);
  } catch (error) {
    await client.query("ROLLBACK");
    throw new Error(`Error inserting coupon: ${error}`);
  } finally {
    client.release();
  }
}

export async function updateCreditsToCoupon(couponData: any) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    const exists = await couponExists(couponData.Id, "CREDIT");
    if (!exists) {
      await insertCreditsToCoupon(couponData);
      return;
    }

    if (couponData.BookingId) {
      const bookingRes = await client.query(
        `select id from booking where legacy_id = $1`,
        [couponData.BookingId]
      );
      couponData.BookingId = bookingRes.rows[0]?.id;
    }

    // Insert credit coupon
    const result = await client.query(
      `UPDATE coupon set 
            description = $1, reason = $2, amount = $3,
            booking_id = $4, updated_at = $5, deleted_at = $6
            where id = $7`,
      [
        couponData.Notes,
        couponData.Notes,
        couponData.Amount,
        couponData.BookingId,
        couponData.UpdatedAt ?? couponData.CreatedAt,
        couponData.DeletedAt,
        exists.id,
      ]
    );

    const couponId = exists.id;

    // Try to fetch customer account after inserting coupon
    const customerAccountId = await getCustomerAccountIdByLegacyId(
      couponData.CustomerId
    );

    if (customerAccountId && couponId) {
      // Create entry in user_coupons only if account exists
      await client.query(`DELETE FROM user_coupons where coupon_id = $1`, [
        couponId,
      ]);
      await client.query(
        `INSERT INTO user_coupons (account_id, coupon_id, updated_at)
           VALUES ($1, $2, NOW())`,
        [customerAccountId, couponId]
      );
    }

    await client.query("COMMIT");
    console.log(`Updated credit coupon ${couponData.CouponCode}`);
  } catch (error) {
    await client.query("ROLLBACK");
    throw new Error(`Error inserting coupon: ${error}`);
  } finally {
    client.release();
  }
}

async function couponExists(couponId: string, couponType: string) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    const result = await client.query(
      `select * from coupon where legacy_id = $1 and coupon_type = $2`,
      [couponId, couponType]
    );

    return result.rows?.[0];
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error inserting coupon:", error);
  } finally {
    client.release();
  }
}
