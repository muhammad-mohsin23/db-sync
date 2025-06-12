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
import { getServiceLineIdFromOptionId } from "./service-line";
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

    const accountId = await getAccountIdByLegacyId(item.CustomerId);

    if (!accountId) {
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

    const bookingDate = item.Start ?? item.CreatedAt;

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

    const [scheduleRows]: any = await mysqlConn.execute(
      `SELECT * FROM schedules WHERE id = ? AND DeletedAt IS NULL`,
      [item?.ScheduleId]
    );

    if (!scheduleRows || scheduleRows.length === 0) {
      await client.query("ROLLBACK");
      console.log(`No schedule data found for id ${item?.ScheduleId}`);
      return;
    }

    const legacyScheduleData = scheduleRows[0];

    if (legacyScheduleData) {
      const account = await client.query(
        `SELECT id,account_type FROM account WHERE legacy_id = $1`,
        [accountId]
      );

      if (account.rows.length === 0) {
        await client.query("ROLLBACK");
        console.log(`No account found with this accountId ${accountId}`);
        return;
      }

      const customerAccounType = serviceResult.rows[0].account_type;
      await client.query(
        `INSERT INTO booking_event (
      booking_id,
      event_type,
      timestamp,
      created_for,
      created_at,
      updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [
          result.rows[0].id, // booking_id
          legacyScheduleData.Type === "Repeat" ? "REPEAT" : "ONE_TIME",
          legacyScheduleData?.Start || bookingDate,
          customerAccounType,
          new Date(), // created_at
          new Date(), // updated_at
        ]
      );
    }
    await client.query(
      `INSERT INTO booking_event (
        booking_id,
        event_type,
        timestamp,
        created_for,
        body,
        created_at,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [
        result.rows[0].id, // booking_id
        "BOOKING_CREATED", // event_type
        legacyScheduleData?.Start || bookingDate, // timestamp
        "RESIDENT", // created_for (assuming default is RESIDENT)
        JSON.stringify(legacyScheduleData), // body
        new Date(), // created_at
        new Date(), // updated_at
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

export async function updateBooking(item: any) {
  const client = await pgPool.connect();
  const mysqlConn = await mysqlConnection();

  try {
    await client.query("BEGIN");

    // Find existing booking by legacy_id
    const existing = await client.query(
      `SELECT id FROM booking WHERE legacy_id = $1`,
      [item.Id]
    );

    if (existing.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(`Booking with legacy_id ${item.Id} not found.`);
      return;
    }

    const bookingId = existing.rows[0].id;

    const accountId = await getAccountIdByLegacyId(item.CustomerId);
    if (!accountId) {
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
      console.log(`Unit with legacy_id ${item.UnitResidentId} not found.`);
      return;
    }

    const [rows]: any = await mysqlConn.execute(
      `SELECT service_line_option_id FROM service_line_option_mapping WHERE option_id = ?`,
      [item?.OptionId]
    );

    if (!rows || rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(`No service_line_option_id for option_id ${item.OptionId}`);
      return;
    }

    const legacyServiceId = rows[0].service_line_option_id;

    const serviceResult = await client.query(
      `SELECT id FROM service WHERE legacy_id = $1`,
      [legacyServiceId]
    );

    if (serviceResult.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(
        `❌ Service not found in PostgreSQL for legacy_id ${legacyServiceId}`
      );
      return;
    }

    const serviceId = serviceResult.rows[0].id;

    const bookingDate = item.Start ?? item.CreatedAt;

    await client.query(
      `UPDATE booking SET
        reference_number = $1,
        account_id = $2,
        property_id = $3,
        service_id = $4,
        date = $5,
        payment_method_id = $6,
        unit_id = $7,
        updated_at = $8
      WHERE legacy_id = $9`,
      [
        `BK-${item.Id}`,
        accountId,
        propertyId,
        serviceId,
        bookingDate,
        item.PaymentTokenId ?? null,
        unitId,
        new Date(),
        item.Id,
      ]
    );

    await client.query("COMMIT");
    console.log(`✅ Booking with legacy_id ${item.Id} updated successfully.`);
    return bookingId;
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error updating booking:", err);
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

export async function updateBookingAddOn(item: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    const bookingId = await getBookingIdByLegacyId(item.BookingId);
    const addOnId = await getAddOnIdByLegacyId(item.AddOnId);

    if (!bookingId || !addOnId) {
      await client.query("ROLLBACK");
      console.warn(
        `Missing booking or add-on reference for legacy ID: ${item.Id}`
      );
      return;
    }

    // Check if the record exists
    const { rows: existingRows } = await client.query(
      `SELECT id FROM booking_addon WHERE legacy_id = $1`,
      [item.Id]
    );

    if (existingRows.length === 0) {
      await client.query("ROLLBACK");
      console.warn(`No booking_addon found with legacy_id ${item.Id}`);
      return;
    }

    await client.query(
      `UPDATE booking_addon
       SET booking_id = $1,
           addon_id = $2,
           updated_at = $3
       WHERE legacy_id = $4`,
      [bookingId, addOnId, new Date(), item.Id]
    );

    await client.query("COMMIT");
    console.log(`Booking add-on (legacy_id: ${item.Id}) updated successfully.`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error(
      `Error updating booking add-on (legacy_id: ${item.Id}):`,
      err
    );
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

export async function updateBookingServiceDetails(bookingData: any) {
  const client = await pgPool.connect();
  const mysqlConn = await mysqlConnection(); // Await the connection

  try {
    await client.query("BEGIN");

    // Get the booking ID from legacy ID
    const bookingId = await getBookingIdByLegacyId(bookingData.BookingId);
    if (!bookingId) {
      await client.query("ROLLBACK");
      console.error(`Booking not found for legacy ID ${bookingData.BookingId}`);
      return;
    }

    // Determine booking status
    const status = mapLegacyStatusToBookingStatus(bookingData.Status);

    // Update booking with status and updated_at
    await client.query(
      `UPDATE booking
       SET status = $1,
           updated_at = NOW()
       WHERE id = $2`,
      [status, bookingId]
    );

    if (bookingData.ClockedIn) {
      await client.query(
        `INSERT INTO status_history (booking_id, status, time, platform, message, created_at)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          bookingId,
          "CLOCK_IN",
          bookingData.ClockedIn,
          null,
          null,
          bookingData.CreatedAt || new Date(),
        ]
      );
    }

    if (bookingData.OnTheWay) {
      await client.query(
        `INSERT INTO status_history (booking_id, status, time, platform, message, created_at)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          bookingId,
          "ON_THE_WAY",
          bookingData.OnTheWay,
          null,
          null,
          bookingData.CreatedAt || new Date(),
        ]
      );
    }

    if (bookingData.ClockedOut) {
      await client.query(
        `INSERT INTO status_history (booking_id, status, time, platform, message, created_at)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          bookingId,
          "COMPLETED",
          bookingData.ClockedOut,
          null,
          null,
          bookingData.CreatedAt || new Date(),
        ]
      );
    }
    // Check if dispatch needs to be updated or created
    // if (bookingData.ServiceProviderManagerId) {
    //   // Get company ID from manager's legacy ID
    //   const companyRes = await client.query(
    //     `SELECT company_id FROM account WHERE legacy_id = $1 AND account_type = 'SERVICE_PROVIDER'`,
    //     [bookingData.ServiceProviderManagerId]
    //   );
    //   const companyId = companyRes.rows?.[0]?.company_id;

    //   if (companyId) {
    //     // Check if dispatch record already exists
    //     const dispatchCheck = await client.query(
    //       `SELECT id FROM dispatch WHERE booking_id = $1`,
    //       [bookingId]
    //     );

    //     const dispatchDate = bookingData.OnTheWay || bookingData.CreatedAt;

    //     if (dispatchCheck.rows.length > 0) {
    //       // Update existing dispatch
    //       const dispatchId = dispatchCheck.rows[0].id;
    //       await client.query(
    //         `UPDATE dispatch
    //          SET company_id = $1,
    //              date = $2,
    //              updated_at = NOW()
    //          WHERE id = $3`,
    //         [companyId, dispatchDate, dispatchId]
    //       );

    //       // Handle dispatch_pro update/insert
    //       if (bookingData.ServiceProviderRunnerId) {
    //         // Get pro account ID
    //         const proAccountRes = await client.query(
    //           `SELECT id FROM account WHERE legacy_id = $1 AND account_type = 'PRO' AND company_id = $2`,
    //           [bookingData.ServiceProviderRunnerId, companyId]
    //         );
    //         const proAccountId = proAccountRes.rows?.[0]?.id;

    //         if (proAccountId) {
    //           // Check if dispatch_pro exists
    //           const dispatchProCheck = await client.query(
    //             `SELECT id FROM dispatch_pro WHERE dispatch_id = $1`,
    //             [dispatchId]
    //           );

    //           if (dispatchProCheck.rows.length > 0) {
    //             // Update existing dispatch_pro
    //             await client.query(
    //               `UPDATE dispatch_pro
    //                SET account_id = $1,
    //                    updated_at = NOW()
    //                WHERE dispatch_id = $2`,
    //               [proAccountId, dispatchId]
    //             );
    //           } else {
    //             // Insert new dispatch_pro
    //             await client.query(
    //               `INSERT INTO dispatch_pro (dispatch_id, account_id, updated_at)
    //                VALUES ($1, $2, NOW())`,
    //               [dispatchId, proAccountId]
    //             );
    //           }
    //         }
    //       }
    //     } else {
    //       // Create new dispatch record
    //       const dispatchInsertResult = await client.query(
    //         `INSERT INTO dispatch (company_id, booking_id, date, updated_at)
    //          VALUES ($1, $2, $3, NOW()) RETURNING id`,
    //         [companyId, bookingId, dispatchDate]
    //       );

    //       const dispatchId = dispatchInsertResult.rows[0].id;

    //       // Insert dispatch_pro if runner exists
    //       if (bookingData.ServiceProviderRunnerId) {
    //         const proAccountRes = await client.query(
    //           `SELECT id FROM account WHERE legacy_id = $1 AND account_type = 'PRO' AND company_id = $2`,
    //           [bookingData.ServiceProviderRunnerId, companyId]
    //         );
    //         const proAccountId = proAccountRes.rows?.[0]?.id;

    //         if (proAccountId) {
    //           await client.query(
    //             `INSERT INTO dispatch_pro (dispatch_id, account_id, updated_at)
    //              VALUES ($1, $2, NOW())`,
    //             [dispatchId, proAccountId]
    //           );
    //         }
    //       }
    //     }
    //   }
    // }

    if (bookingData.ServiceProviderCompanyId) {
      // Step 1: Get management company ID from legacy ID
      const managementCompanyRes = await client.query(
        `SELECT id FROM service_provider_management_companies WHERE legacy_id = $1`,
        [bookingData.ServiceProviderCompanyId]
      );
      const managementCompanyId = managementCompanyRes.rows?.[0]?.id;

      // Step 2: Get company ID under management company
      let companyId: string | undefined;
      if (managementCompanyId) {
        const companyRes = await client.query(
          `SELECT id FROM company WHERE service_provider_management_company_id = $1 AND type = 'SERVICE_PROVIDER'`,
          [managementCompanyId]
        );
        companyId = companyRes.rows?.[0]?.id;
      }

      if (companyId) {
        // Step 3: Check for existing dispatch
        const dispatchRes = await client.query(
          `SELECT id FROM dispatch WHERE company_id = $1 AND booking_id = $2`,
          [companyId, bookingId]
        );

        const dispatchDate = bookingData.Start || bookingData.bCreatedAt;
        const createdAt =
          !bookingData.CreatedAt ||
          bookingData.CreatedAt === "0000-00-00 00:00:00"
            ? new Date()
            : new Date(bookingData.CreatedAt);

        let dispatchId: string;
        if (dispatchRes.rows.length > 0) {
          dispatchId = dispatchRes.rows[0].id;
        } else {
          const insertDispatchRes = await client.query(
            `INSERT INTO dispatch (company_id, booking_id, date, created_at, updated_at)
             VALUES ($1, $2, $3, $4, NOW())
             RETURNING id`,
            [companyId, bookingId, dispatchDate, createdAt]
          );
          dispatchId = insertDispatchRes.rows[0].id;
        }

        // Step 4: Upsert dispatch_pro if runner exists
        if (bookingData.ServiceProviderRunnerId) {
          // Step 4.1: Fetch userId from MySQL using ServiceProviderRunnerId
          const [rows]: any = await mysqlConn.query(
            `SELECT userId FROM service_provider_runner WHERE id = ?`,
            [bookingData.ServiceProviderRunnerId]
          );
          const runnerUserId = rows?.[0]?.userId;

          if (runnerUserId) {
            // Step 4.2: Use runnerUserId as legacy_id to find PRO account in PostgreSQL
            const proAccountRes = await client.query(
              `SELECT id FROM account WHERE account_type = 'PRO' AND legacy_id = $1`,
              [runnerUserId]
            );
            const proAccountId = proAccountRes.rows?.[0]?.id;
            if (proAccountId) {
              await client.query(
                `INSERT INTO dispatch_pro (dispatch_id, account_id, created_at, updated_at)
               VALUES ($1, $2, $3, NOW())
               ON CONFLICT (dispatch_id, account_id)
               DO UPDATE SET updated_at = NOW()`,
                [dispatchId, proAccountId, createdAt]
              );
            }
          }
        }
      }
    }

    await client.query("COMMIT");
    console.log(
      `Booking ID ${bookingId} service details updated successfully.`
    );
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error updating booking service details:", error);
  } finally {
    client.release();
  }
}

export async function insertBookingTimeWindow(timeWindowData: any) {
  const client = await pgPool.connect();
  const mysqlConn = await mysqlConnection(); // Await the connection

  try {
    await client.query("BEGIN");

    const serviceLineResult = await getServiceLineIdFromOptionId(
      mysqlConn,
      timeWindowData.OptionId
    );

    if (!serviceLineResult) {
      await client.query("ROLLBACK");
      console.log(
        `❌ Could not resolve service line data for option_id ${timeWindowData.OptionId}`
      );
      return;
    }

    const { serviceLineId } = serviceLineResult;

    const pgServiceLineResult = await client.query(
      `SELECT id FROM service_line WHERE legacy_id = $1`,
      [serviceLineId]
    );

    if (pgServiceLineResult.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(
        `❌ No PostgreSQL service_line found for legacy_id ${serviceLineId}`
      );
      return;
    }

    const pgServiceLineId = pgServiceLineResult.rows[0].id;

    // Insert into PostgreSQL time_window table
    const result = await client.query(
      `INSERT INTO time_window (
        legacy_id,
        name,
        start_time,
        end_time,
        service_line_id
        created_at,
        updated_at,
        deleted_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      RETURNING id`,
      [
        timeWindowData.Id,
        timeWindowData.DayOfWeek,
        timeWindowData.StartTime,
        timeWindowData.EndTime,
        pgServiceLineId,
        timeWindowData.CreatedAt,
        timeWindowData.UpdatedAt,
        timeWindowData.DeletedAt || null,
      ]
    );

    await client.query("COMMIT");
    console.log(`✅ Time window inserted with ID: ${result.rows[0].id}`);
    return;
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error inserting time window:", err);
  } finally {
    client.release();
  }
}

export async function updateBookingTimeWindow(timeWindowData: any) {
  const client = await pgPool.connect();
  const mysqlConn = await mysqlConnection(); // Await the connection

  try {
    await client.query("BEGIN");

    const serviceLineResult = await getServiceLineIdFromOptionId(
      mysqlConn,
      timeWindowData.OptionId
    );

    if (!serviceLineResult) {
      await client.query("ROLLBACK");
      console.log(
        `❌ Could not resolve service line data for option_id ${timeWindowData.OptionId}`
      );
      return;
    }

    const { serviceLineId } = serviceLineResult;

    const pgServiceLineResult = await client.query(
      `SELECT id FROM service_line WHERE legacy_id = $1`,
      [serviceLineId]
    );

    if (pgServiceLineResult.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(
        `❌ No PostgreSQL service_line found for legacy_id ${serviceLineId}`
      );
      return;
    }

    const pgServiceLineId = pgServiceLineResult.rows[0].id;

    // Update the time_window in PostgreSQL
    await client.query(
      `UPDATE time_window
       SET name = $1,
           start_time = $2,
           end_time = $3,
           service_line_id = $4,
           updated_at = NOW()
           deleted_at = $5
       WHERE legacy_id = $5`,
      [
        timeWindowData.DayOfWeek,
        timeWindowData.StartTime,
        timeWindowData.EndTime,
        pgServiceLineId,
        timeWindowData.Id,
        timeWindowData.deleted_at ?? null,
      ]
    );

    await client.query("COMMIT");
    console.log(`✅ Time window with legacy_id ${timeWindowData.Id} updated.`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error updating time window:", err);
  } finally {
    client.release();
  }
}

export async function insertOneTimeScheduleWindow(scheduleData: any) {
  const client = await pgPool.connect();
  const mysqlConn = await mysqlConnection();

  try {
    await client.query("BEGIN");

    // 1. Fetch one-time schedule data from MySQL (with deleted_at check)
    const [oneTimeRows]: any = await mysqlConn.execute(
      `SELECT * FROM onetimeschedules WHERE id = ? AND DeletedAt IS NULL`,
      [scheduleData?.OneTimeScheduleId]
    );

    if (!oneTimeRows || oneTimeRows.length === 0) {
      await client.query("ROLLBACK");
      console.log(
        `❌ No one-time schedule found with id ${scheduleData?.OneTimeScheduleId}`
      );
      return;
    }

    const oneTimeSchedule = oneTimeRows[0];

    // 2. Get the booking_id from the one-time schedule
    const bookingId = await getBookingIdByLegacyId(oneTimeSchedule.BookingId);

    if (!bookingId) {
      await client.query("ROLLBACK");
      console.log(
        `❌ Booking not found for legacy_id ${oneTimeSchedule.BookingId}`
      );
      return;
    }

    // 3. Get the assigned_time_window_id from time_window using legacy_id
    const timeWindowResult = await client.query(
      `SELECT id FROM time_window WHERE legacy_id = $1 AND deleted_at IS NULL`,
      [scheduleData?.BookingWindowId]
    );

    if (timeWindowResult.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(
        `❌ No time_window found with legacy_id ${scheduleData?.BookingWindowId}`
      );
      return;
    }

    const timeWindowId = timeWindowResult.rows[0].id;

    // 4. Insert into booking_event
    await client.query(
      `UPDATE booking_event
       SET assigned_time_window_id = $1,
           updated_at = $2
       WHERE booking_id = $3
         AND deleted_at is NULL`,
      [
        timeWindowId, // $1
        new Date(), // $2
        bookingId, // $3
      ]
    );

    await client.query("COMMIT");
    console.log(`✅ One-time schedule inserted for booking ID ${bookingId}`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error inserting one-time schedule:", err);
  } finally {
    client.release();
  }
}

export async function updateOneTimeScheduleWindow(scheduleData: any) {
  const client = await pgPool.connect();
  const mysqlConn = await mysqlConnection();

  try {
    await client.query("BEGIN");

    // 1. Fetch one-time schedule data from MySQL (with deleted_at check)
    const [oneTimeRows]: any = await mysqlConn.execute(
      `SELECT * FROM onetimeschedules WHERE id = ? AND DeletedAt IS NULL`,
      [scheduleData?.OneTimeScheduleId]
    );

    if (!oneTimeRows || oneTimeRows.length === 0) {
      await client.query("ROLLBACK");
      console.log(
        `❌ No one-time schedule found with id ${scheduleData?.OneTimeScheduleId}`
      );
      return;
    }

    const oneTimeSchedule = oneTimeRows[0];

    // 2. Get the booking_id from the one-time schedule
    const bookingId = await getBookingIdByLegacyId(oneTimeSchedule.BookingId);

    if (!bookingId) {
      await client.query("ROLLBACK");
      console.log(
        `❌ Booking not found for legacy_id ${oneTimeSchedule.BookingId}`
      );
      return;
    }

    // 3. Get the assigned_time_window_id from time_window using legacy_id
    const timeWindowResult = await client.query(
      `SELECT id FROM time_window WHERE legacy_id = $1 AND deleted_at IS NULL`,
      [scheduleData?.BookingWindowId]
    );

    if (timeWindowResult.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(
        `❌ No time_window found with legacy_id ${scheduleData?.BookingWindowId}`
      );
      return;
    }

    const timeWindowId = timeWindowResult.rows[0].id;

    // 4. Check if a booking_event exists for this booking_id and event_type ONE_TIME_SCHEDULE
    const eventCheck = await client.query(
      `SELECT id FROM booking_event
       WHERE booking_id = $1
         AND deleted_at IS NULL`,
      [bookingId]
    );

    if (eventCheck.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(
        `❌ No booking_event found for booking_id ${bookingId} with event_type 'ONE_TIME_SCHEDULE'`
      );
      return;
    }

    // 5. Perform the update
    await client.query(
      `UPDATE booking_event
       SET assigned_time_window_id = $1,
           updated_at = $2
       WHERE booking_id = $3
         AND deleted_at IS NULL`,
      [
        timeWindowId, // $1
        new Date(), // $2
        bookingId, // $3
      ]
    );

    await client.query("COMMIT");
    console.log(`✅ One-time schedule UPDATED for booking ID ${bookingId}`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error updating one-time schedule:", err);
  } finally {
    client.release();
  }
}
