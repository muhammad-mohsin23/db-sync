import { pgPool } from "../database/database.service";
import { getAccountIdByLegacyId } from "./customer";

export async function insertCoupon(couponData: any) {
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
    const customerAccountId = await getAccountIdByLegacyId(
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
