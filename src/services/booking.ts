import { mysqlConnection, pgPool } from "../database/database.service";
import {
  expandBookingRange,
  getTotalDaysFromRecurrence,
  mapEventNameToBookingStatus,
  mapLegacyStatusToBookingStatus,
  Recurrence,
} from "../helpers/util.herlper";
import { getAccountIdByLegacyId } from "./customer";
import { getPropertyIdByLegacyId } from "./property";
import { getUnitFromUnitResident } from "./unit";

export async function insertBooking(item: any) {
  const client = await pgPool.connect();
  const mysqlConn = await mysqlConnection();

  try {
    await client.query("BEGIN");

    // Check if booking already exists by legacy_id
    const existing = await client.query(
      `SELECT id FROM booking WHERE legacy_id = $1`,
      [item.Id]
    );

    if (existing.rows.length > 0) {
      await client.query("ROLLBACK");
      console.log(`ℹ️ Booking with legacy_id ${item.Id} already exists.`);
      return;
    }

    const existingAccount = await client.query(
      `SELECT id FROM account WHERE legacy_id = $1`,
      [item.CustomerId]
    );
    const accountId = await getAccountIdByLegacyId(item.CustomerId);

    if (!existingAccount) {
      await client.query("ROLLBACK");
      console.log(`Account with legacy_id ${item.CustomerId} not found.`);
      return;
    }

    const propertyId = await getPropertyIdByLegacyId(item.PropertyId);

    if (!propertyId) {
      await client.query("ROLLBACK");
      console.log(`Property with legacy_id ${item.PropertyId} not found.`);
      return;
    }

    const unitId = await getUnitFromUnitResident(item.UnitResidentId);
    if (!unitId) {
      await client.query("ROLLBACK");
      console.log(
        `Unit with legacy_id ${item.UnitResidentId} not found in unit_resident.`
      );
      return;
    }

    const [rows]: any = await mysqlConn.execute(
      `SELECT service_line_option_id FROM service_line_option_mapping WHERE option_id = ?`,
      [item?.OptionId]
    );

    if (!rows || rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(
        `No service_line_option_id found for option_id ${item.OptionId}`
      );
      return;
    }

    const legacyServiceId = rows[0].service_line_option_id;

    // Step 2: Lookup actual service ID from PostgreSQL using legacy ID
    const serviceResult = await client.query(
      `SELECT id FROM service WHERE legacy_id = $1`,
      [legacyServiceId]
    );

    if (serviceResult.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(
        `No service found in PostgreSQL for legacy_id ${legacyServiceId}`
      );
      return;
    }

    const serviceId = serviceResult.rows[0].id;

    const bookingDate = item.Start ?? item.bCreatedAt;

    const result = await client.query(
      `INSERT INTO booking (
          reference_number,
          account_id,
          property_id,
          service_id,
          date,
          payment_method_id,
          unit_id,
          legacy_id,
          updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id`,
      [
        `BK-${item.Id}`,
        accountId,
        propertyId,
        serviceId,
        bookingDate,
        item.PaymentTokenId ?? null,
        unitId,
        item.Id,
        new Date(),
      ]
    );

    await client.query("COMMIT");
    console.log(`Booking inserted with ID: ${result.rows[0].id}`);
    return result.rows[0].id;
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Error inserting booking:", err);
  } finally {
    client.release();
  }
}

export async function insertBookingAddOns(item: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    const bookingId = await getBookingIdByLegacyId(item.BookingId);
    const addOnId = await getAddOnIdByLegacyId(item.AddOnId);

    if (!bookingId || !addOnId) {
      await client.query("ROLLBACK");
      return;
    }

    // Check if the record already exists
    const existing = await client.query(
      `SELECT id FROM booking_addon WHERE booking_id = $1 AND addon_id = $2`,
      [bookingId, addOnId]
    );

    if (existing.rows.length > 0) {
      await client.query("ROLLBACK");
      console.log(
        `Booking add-on already exists for booking ${item.BookingId} and addon ${item.AddOnId}`
      );
      return;
    }

    await client.query(
      `INSERT INTO booking_addon (
          booking_id, addon_id, legacy_id, updated_at
        ) VALUES ($1, $2, $3, $4, $5)`,
      [bookingId, addOnId, item.Id, new Date()]
    );

    await client.query("COMMIT");
    console.log("Booking add-on inserted successfully.");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Error inserting booking add-on:", err);
  } finally {
    client.release();
  }
}

export async function getBookingIdByLegacyId(legacyId: number) {
  const client = await pgPool.connect();

  try {
    const res = await client.query(
      `SELECT id FROM booking WHERE legacy_id = $1`,
      [legacyId]
    );

    if (res.rows.length === 0) {
      console.log(`Booking with legacy_id ${legacyId} not found.`);
      return null;
    }

    return res.rows[0].id;
  } catch (err) {
    console.error("Error fetching booking by legacy ID:", err);
    return null;
  } finally {
    client.release();
  }
}

export async function getAddOnIdByLegacyId(legacyId: number) {
  const client = await pgPool.connect();

  try {
    const res = await client.query(
      `SELECT id FROM addon WHERE legacy_id = $1`,
      [legacyId]
    );

    if (res.rows.length === 0) {
      console.log(`Add-on with legacy_id ${legacyId} not found.`);
      return null;
    }

    return res.rows[0].id;
  } catch (err) {
    console.error("Error fetching add-on by legacy ID:", err);
    return null;
  } finally {
    client.release();
  }
}

//Insert in status history in pg table
export async function insertBookingActivity(item: any) {
  const client = await pgPool.connect();
  const bookingId = await getBookingIdByLegacyId(item.BookingId);

  const message = `${item.Time ?? ""} ${item.EventName ?? ""} ${
    item.Detail?.service_name ?? ""
  }`.trim();

  const bookingEventName = mapEventNameToBookingStatus(item.EventName);

  try {
    await client.query(
      `INSERT INTO status_history (
          booking_id, status, time, platform, message, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        bookingId,
        bookingEventName || null,
        item.Time || null,
        item.Platform || null,
        message,
        new Date(),
      ]
    );

    console.log(`✅ Status history inserted for booking ID ${bookingId}`);
  } catch (err) {
    console.error("❌ Error inserting status history:", err);
  } finally {
    client.release();
  }
}

export async function insertRecurringScheduleItem(recurringData: any) {
  const mysqlConn = await mysqlConnection();
  const client = await pgPool.connect();
  try {
    const [rows]: any[] = await mysqlConn.execute(
      "SELECT Id, Interval, Frequency FROM recurrence WHERE Id = ?",
      [recurringData.RecurrenceId]
    );

    if (!rows || rows.length === 0) {
      throw new Error(
        `Recurrence with ID ${recurringData.RecurrenceId} not found`
      );
    }
    const recurrence: Recurrence = rows[0];
    const recurrenceData = {
      Id: recurrence.Id,
      Interval: recurrence.Interval,
      Frequency: recurrence.Frequency,
    };
    const totalDays = getTotalDaysFromRecurrence(recurrenceData);

    await client.query("BEGIN");

    const bookingId = await getBookingIdByLegacyId(recurringData.BookingId);
    await client.query(
      `UPDATE bookings SET repeat_interval = $1 WHERE id = $2`,
      [totalDays, bookingId]
    );

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Error inserting recurring schedule:", err);
  } finally {
    client.release();
  }
}

export async function updateRepeatedBookings(repeatBookingData: any) {
  const client = await pgPool.connect();
  try {
    const { record_id, new_data } = repeatBookingData;
    const { BookingId, OriginalBookingId } = JSON.parse(new_data);

    const bookingIds: number[] = expandBookingRange(record_id);
    const bookingsToUpdate = bookingIds.filter(
      (id) => id !== OriginalBookingId
    );

    await client.query("BEGIN");

    // Get repeat_interval of original booking
    const { rows: originalRows } = await client.query(
      `SELECT repeat_interval FROM bookings WHERE legacy_id = $1`,
      [OriginalBookingId]
    );

    if (!originalRows.length) {
      throw new Error(`Original booking ${OriginalBookingId} not found`);
    }

    const repeatInterval = originalRows[0].repeat_interval;

    // Update each repeated booking
    for (const bookingLegacyId of bookingsToUpdate) {
      const { rows: targetRows } = await client.query(
        `SELECT id FROM bookings WHERE legacy_id = $1`,
        [bookingLegacyId]
      );

      if (!targetRows.length) {
        console.warn(`Booking ${bookingLegacyId} not found in pg`);
        continue;
      }

      await client.query(
        `UPDATE bookings SET repeat_interval = $1, source_booking_id = (
          SELECT id FROM bookings WHERE legacy_id = $2
        ) WHERE legacy_id = $3`,
        [repeatInterval, OriginalBookingId, bookingLegacyId]
      );
    }

    await client.query("COMMIT");
    console.log(
      `Repeat interval updated for bookings: ${bookingsToUpdate.join(", ")}`
    );
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Error updating repeated bookings:", err);
  } finally {
    client.release();
  }
}

export async function insertBookingFeedback(feedbackData: any) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    const bookingId = await getBookingIdByLegacyId(feedbackData.BookingId);
    const accountId = await getAccountIdByLegacyId(feedbackData.CustomerId);

    if (!bookingId) {
      await client.query("ROLLBACK");
      console.log(
        `Booking with legacy_id ${feedbackData.BookingId} not found.`
      );
      return;
    }

    if (!accountId) {
      await client.query("ROLLBACK");
      console.log(
        `Booking with legacy_id ${feedbackData.CustomerId} not found.`
      );
      return;
    }

    await client.query(
      `INSERT INTO booking_feedback (
          booking_id,accountId, rating, comment, updated_at
        ) VALUES ($1, $2, $3, $4)`,
      [
        bookingId,
        accountId,
        feedbackData.Rating,
        feedbackData.Feedback,
        new Date(),
      ]
    );

    await client.query("COMMIT");
    console.log("Booking feedback inserted successfully.");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Error inserting booking feedback:", err);
  } finally {
    client.release();
  }
}

export async function updateBookingFeedback(feedbackData: any) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    const bookingId = await getBookingIdByLegacyId(feedbackData.BookingId);
    const accountId = await getAccountIdByLegacyId(feedbackData.CustomerId);

    if (!bookingId) {
      await client.query("ROLLBACK");
      console.log(
        `Booking with legacy_id ${feedbackData.BookingId} not found.`
      );
      return;
    }

    if (!accountId) {
      await client.query("ROLLBACK");
      console.log(
        `Account with legacy_id ${feedbackData.CustomerId} not found.`
      );
      return;
    }

    // Check if the feedback already exists
    const existing = await client.query(
      `SELECT id FROM booking_feedback WHERE booking_id = $1 AND accountId = $2`,
      [bookingId, accountId]
    );

    if (existing.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log("No existing feedback found for this booking and account.");
      return;
    }

    // Perform the update
    await client.query(
      `UPDATE booking_feedback
       SET rating = $1, comment = $2, updated_at = $3
       WHERE booking_id = $4 AND accountId = $5`,
      [
        feedbackData.Rating,
        feedbackData.Feedback,
        new Date(),
        bookingId,
        accountId,
      ]
    );

    await client.query("COMMIT");
    console.log("✅ Booking feedback updated successfully.");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error updating booking feedback:", err);
  } finally {
    client.release();
  }
}

export async function insertBookingServiceDetails(bookingData: any) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    const bookingId = await getBookingIdByLegacyId(bookingData.BookingId);
    if (!bookingId) {
      await client.query("ROLLBACK");
      console.error(`Booking not found for legacy ID ${bookingData.BookingId}`);
      return;
    }

    // Determine booking status
    const status = mapLegacyStatusToBookingStatus(bookingData.Status);

    // Update booking with status and notes
    await client.query(
      `UPDATE booking
       SET status = $1,
           updated_at = NOW()
       WHERE id = $2`,
      [status, bookingId]
    );

    let companyId = null;
    if (bookingData.ServiceProviderManagerId) {
      const res = await client.query(
        `SELECT company_id FROM account WHERE legacy_id = $1 AND account_type = 'SERVICE_PROVIDER'`,
        [bookingData.ServiceProviderManagerId]
      );
      companyId = res.rows?.[0]?.company_id;
    }

    // Insert into dispatch if company exists
    if (companyId) {
      const dispatchDate = bookingData.Start || bookingData.CreatedAt;
      const dispatchInsertResult = await client.query(
        `INSERT INTO dispatch (company_id, booking_id, date, updated_at)
         VALUES ($1, $2, $3, NOW()) RETURNING id`,
        [companyId, bookingId, dispatchDate]
      );

      const dispatchId = dispatchInsertResult.rows[0].id;

      // Insert dispatch_pro if runner ID exists
      if (bookingData.ServiceProviderRunnerId) {
        const runnerAccountRes = await client.query(
          `SELECT id FROM account WHERE legacy_id = $1 AND account_type = 'PRO' AND company_id = $2`,
          [bookingData.ServiceProviderRunnerId, companyId]
        );
        const proAccountId = runnerAccountRes.rows?.[0]?.id;

        if (proAccountId) {
          await client.query(
            `INSERT INTO dispatch_pro (dispatch_id, account_id, updated_at)
             VALUES ($1, $2, NOW())`,
            [dispatchId, proAccountId]
          );
        }
      }
    }

    await client.query("COMMIT");
    console.log(`Booking ID ${bookingId} updated from kes.`);
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error updating booking from kes:", error);
  } finally {
    client.release();
  }
}
