import { pgPool } from "../database/database.service";
import { getBookingIdByLegacyId } from "./booking";

export async function insertInvoice(invoiceData: any, mysqlConn: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    // Get booking UUID from legacy BookingId
    const bookingId = await getBookingIdByLegacyId(invoiceData.BookingId);

    if (!bookingId) {
      await client.query("ROLLBACK");
      throw new Error(
        `Booking with legacy_id ${invoiceData.BookingId} not found.`
      );
      // return;
    }

    // Check if invoice already exists
    const existing = await client.query(
      `SELECT id FROM invoice WHERE legacy_id = $1`,
      [invoiceData.Id]
    );

    if (existing.rows.length > 0) {
      await client.query("ROLLBACK");
      console.log(`Invoice with legacy_id ${invoiceData.Id} already exists.`);
      throw new Error(
        `Invoice with legacy_id ${invoiceData.Id} already exists.`
      );
      // return;
    }

    await client.query(
      `INSERT INTO invoice (
          invoice_number, booking_id, brain_tree_id, disbursement_id,
          refund_id, status, refund_status, refunded_at,
          transaction_id, type, pre_authorization_id, legacy_id,
          created_at,
           updated_at
        ) VALUES (
          $1, $2, $3, $4,
          $5, $6, $7, $8,
          $9, $10, $11, $12,
          $13,$14
        )`,
      [
        `INV-${invoiceData.Id}`,
        bookingId,
        invoiceData.BraintreeId || null,
        invoiceData.DisbursementId || null,
        invoiceData.RefundId || null,
        invoiceData.Status,
        invoiceData.RefundStatus,
        invoiceData.RefundedAt || null,
        invoiceData.TransactionId || null,
        invoiceData.Type || null,
        invoiceData.PreAuthorizationId || null,
        invoiceData.Id,
        invoiceData.CreatedAt ?? new Date(),
        new Date(),
      ]
    );

    await client.query("COMMIT");
    console.log("✅ Invoice inserted successfully.");
  } catch (err) {
    await client.query("ROLLBACK");
    throw new Error(`Error inserting invoice`);
  } finally {
    client.release();
  }
}

export async function insertInvoiceItem(invoiceItem: any, mysqlConn: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    // Get invoice_id (UUID) from legacy invoice ID
    const invoiceId = await getInvoiceIdByLegacyId(invoiceItem.InvoiceId);
    console.log("Invoice ID:", invoiceId);

    if (!invoiceId) {
      await client.query("ROLLBACK");
      console.log(`Invoice with legacy_id ${invoiceItem.InvoiceId} not found.`);
      // return;
      throw new Error(
        `Invoice with legacy_id ${invoiceItem.InvoiceId} not found.`
      );
    }

    // Check for duplicate invoice item
    const existing = await client.query(
      `SELECT id FROM invoice_items WHERE legacy_id = $1 AND invoice_id = $2`,
      [invoiceItem.id, invoiceId]
    );

    if (existing.rows.length > 0) {
      await client.query("ROLLBACK");
      console.log(
        `Invoice item already exists for legacy_id ${invoiceItem.invoiceLineItemId}`
      );
      // return;
      throw new Error(
        `Invoice item already exists for legacy_id ${invoiceItem.invoiceLineItemId}`
      );
    }

    await client.query(
      `INSERT INTO invoice_items (
          invoice_id, price, title, type, legacy_id,created_at,
           updated_at
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6,$7
        )`,
      [
        invoiceId,
        Number(invoiceItem.Amount) || 0,
        invoiceItem.Title,
        invoiceItem.Type,
        invoiceItem.Id,
        invoiceItem.CreatedAt ?? new Date(),
        new Date(),
      ]
    );

    await client.query("COMMIT");
    console.log("✅ Invoice item inserted successfully.");
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("❌ Error inserting invoice item:", err);
    throw new Error(`Error inserting invoice item: ${err.message}`);
  } finally {
    client.release();
  }
}

export async function getInvoiceIdByLegacyId(invoiceLegacyId: number) {
  const client = await pgPool.connect();

  try {
    const res = await client.query(
      `SELECT id FROM invoice WHERE legacy_id = $1`,
      [invoiceLegacyId]
    );

    if (res.rows.length === 0) {
      console.log(`Invoice with legacy_id ${invoiceLegacyId} not found.`);
      return null;
    }

    return res.rows[0].id;
  } catch (err) {
    console.error("Error fetching invoice by legacy ID:", err);
    return null;
  } finally {
    client.release();
  }
}

export async function updateInvoiceItem(
  invoiceItem: any,
  mysqlConn: any,
  id?: any
) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    // Get invoice_id (UUID) from legacy invoice ID
    const invoiceId = await getInvoiceIdByLegacyId(invoiceItem.InvoiceId);

    if (!invoiceId) {
      await client.query("ROLLBACK");
      console.log(`Invoice with legacy_id ${invoiceItem.InvoiceId} not found.`);
      throw new Error(
        `Invoice with legacy_id ${invoiceItem.InvoiceId} not found.`
      );
      // return;
    }

    // Check if the invoice item exists
    const existing = await client.query(
      `SELECT id FROM invoice_items WHERE legacy_id = $1 AND invoice_id = $2`,
      [invoiceItem.invoiceLineItemId, invoiceId]
    );

    if (existing.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(
        `Invoice item with legacy_id ${invoiceItem.invoiceLineItemId} not found.`
      );
      throw new Error(
        `Invoice item with legacy_id ${invoiceItem.invoiceLineItemId} not found.`
      );
      // return;
    }

    // Proceed to update the invoice item
    await client.query(
      `UPDATE invoice_items SET
        price = $1,
        title = $2,
        type = $3,
        updated_at = $4,
        deleted_at = $5
       WHERE legacy_id = $6 AND invoice_id = $7`,
      [
        invoiceItem.Amount || 0,
        invoiceItem.Title,
        invoiceItem.Type,
        new Date(),
        invoiceItem.DeletedAt ? new Date(invoiceItem.DeletedAt) : null,
        invoiceItem.invoiceLineItemId,
        invoiceId,
      ]
    );

    await client.query("COMMIT");
    console.log("✅ Invoice item updated successfully.");
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("❌ Error updating invoice item:", err);
    throw new Error(`Error updating invoice item: ${err.message}`);
  } finally {
    client.release();
  }
}

export async function updateInvoice(
  invoiceData: any,
  mysqlConn: any,
  id?: any
) {
  console.log("Inside updateInvoice function with data:");

  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");
    console.log("Begin updateInvoice function with data:");

    const bookingId = await getBookingIdByLegacyId(invoiceData.BookingId);
    console.log("Booking ID found :", bookingId);

    if (!bookingId) {
      await client.query("ROLLBACK");
      console.log(`Booking with legacy_id ${invoiceData.BookingId} not found.`);
      throw new Error(
        `Booking with legacy_id ${invoiceData.BookingId} not found.`
      );
      // return;
    }

    // Check if invoice exists
    const existing = await client.query(
      `SELECT id FROM invoice WHERE legacy_id = $1`,
      [invoiceData.Id]
    );

    if (existing.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(`Invoice with legacy_id ${invoiceData.Id} not found.`);
      throw new Error(`Invoice with legacy_id ${invoiceData.Id} not found.`);
      // return;
    }

    await client.query(
      `UPDATE invoice SET
        booking_id = $1,
        brain_tree_id = $2,
        disbursement_id = $3,
        refund_id = $4,
        status = $5,
        refund_status = $6,
        refunded_at = $7,
        transaction_id = $8,
        type = $9,
        pre_authorization_id = $10,
        updated_at = $11,
        deleted_at = $12
       WHERE legacy_id = $13`,
      [
        bookingId,
        invoiceData.BraintreeId || null,
        invoiceData.DisbursementId || null,
        invoiceData.RefundId || null,
        invoiceData.Status,
        invoiceData.RefundStatus,
        invoiceData.RefundedAt || null,
        invoiceData.TransactionId || null,
        invoiceData.Type || null,
        invoiceData.PreAuthorizationId || null,
        invoiceData.updatedAt ?? new Date(),
        invoiceData.DeletedAt ? invoiceData.DeletedAt : null,
        invoiceData.Id,
      ]
    );

    await client.query("COMMIT");
    console.log("✅ Invoice updated successfully.");
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("❌ Error updating invoice:", err);
    throw new Error(`Error updating invoice: ${err.message}`);
  } finally {
    console.log("test");
    client.release();
  }
}
