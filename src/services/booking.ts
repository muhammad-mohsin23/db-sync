import { format } from "path";
import { mysqlConnection, pgPool } from "../database/database.service";
import {
  classifyTimeRange,
  expandBookingRange,
  formatDateToSixDecimals,
  getServiceLineUuid,
  getTotalDaysFromRecurrence,
  mapEventNameToBookingStatus,
  mapLegacyStatusToBookingStatus,
  Recurrence,
} from "../helpers/util.herlper";
// import { logger } from "../logging/logging";
import { getCustomerAccountIdByLegacyId } from "./customer";
import { getPropertyIdByLegacyId } from "./property";
import { getServiceLineIdFromOptionId } from "./service-line";
import { getUnitFromUnitResident } from "./unit";
import bcrypt from "bcrypt";

// export async function insertBooking(item: any, mysqlConn: any) {
//   const client = await pgPool.connect();

//   try {
//     await client.query("BEGIN");

//     // Check if booking already exists by legacy_id
//     const existing = await client.query(
//       `SELECT id FROM booking WHERE legacy_id = $1`,
//       [item.Id]
//     );

//     if (existing.rows.length > 0) {
//       await client.query("ROLLBACK");
//       console.log(`ℹ️ Booking with legacy_id ${item.Id} already exists.`);
//       throw new Error(`Booking with legacy_id ${item.Id} already exists.`);
//       // return;
//     }

//     const accountId = await getAccountIdByLegacyId(item.CustomerId);

//     if (!accountId) {
//       await client.query("ROLLBACK");
//       console.log(`Account with legacy_id ${item.CustomerId} not found.`);
//       // return;
//       throw new Error(`Account with legacy_id ${item.CustomerId} not found.`);
//     }

//     const propertyId = await getPropertyIdByLegacyId(item.PropertyId);

//     if (!propertyId) {
//       await client.query("ROLLBACK");
//       console.log(`Property with legacy_id ${item.PropertyId} not found.`);
//       throw new Error(`Property with legacy_id ${item.PropertyId} not found.`);
//       // return;
//     }

//     const unitId = await getUnitFromUnitResident(item.UnitResidentId);
//     if (!unitId) {
//       await client.query("ROLLBACK");
//       console.log(
//         `Unit with legacy_id ${item.UnitResidentId} not found in unit_resident.`
//       );
//       throw new Error(
//         `Unit with legacy_id ${item.UnitResidentId} not found in unit_resident.`
//       );
//       // return;
//     }

//     const [rows]: any = await mysqlConn.execute(
//       `SELECT service_line_option_id FROM service_line_option_mapping WHERE option_id = ?`,
//       [item?.OptionId]
//     );

//     if (!rows || rows.length === 0) {
//       await client.query("ROLLBACK");
//       console.log(
//         `No service_line_option_id found for option_id ${item.OptionId}`
//       );
//       throw new Error(
//         `No service_line_option_id found for option_id ${item.OptionId}`
//       );
//       // return;
//     }

//     const legacyServiceId = rows[0].service_line_option_id;

//     // Step 2: Lookup actual service ID from PostgreSQL using legacy ID
//     const serviceResult = await client.query(
//       `SELECT id FROM service WHERE legacy_id = $1`,
//       [legacyServiceId]
//     );

//     if (serviceResult.rows.length === 0) {
//       await client.query("ROLLBACK");
//       console.log(
//         `No service found in PostgreSQL for legacy_id ${legacyServiceId}`
//       );
//       // return;
//       throw new Error(`Service not found for legacy_id ${legacyServiceId}`);
//     }

//     const serviceId = serviceResult.rows[0].id;

//     const bookingDate = item.Start ?? item.CreatedAt;

//     const result = await client.query(
//       `INSERT INTO booking (
//           reference_number,
//           account_id,
//           property_id,
//           service_id,
//           date,
//           payment_method_id,
//           unit_id,
//           legacy_id,
//           updated_at
//         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
//         RETURNING id`,
//       [
//         `BK-${item.Id}`,
//         accountId,
//         propertyId,
//         serviceId,
//         bookingDate,
//         item.PaymentTokenId ?? null,
//         unitId,
//         item.Id,
//         new Date(),
//       ]
//     );

//     const [scheduleRows]: any = await mysqlConn.execute(
//       `SELECT * FROM schedules WHERE id = ? AND DeletedAt IS NULL`,
//       [item?.ScheduleId]
//     );

//     if (!scheduleRows || scheduleRows.length === 0) {
//       await client.query("ROLLBACK");
//       console.log(`No schedule data found for id ${item?.ScheduleId}`);
//       // return;
//       throw new Error(`No schedule data found for id ${item?.ScheduleId}`);
//     }

//     const legacyScheduleData = scheduleRows[0];

//     if (legacyScheduleData) {
//       const account = await client.query(
//         `SELECT id,account_type FROM account WHERE legacy_id = $1`,
//         [accountId]
//       );

//       if (account.rows.length === 0) {
//         await client.query("ROLLBACK");
//         console.log(`No account found with this accountId ${accountId}`);
//         // return;
//         throw new Error(`No account found with this accountId ${accountId}`);
//       }

//       const customerAccounType = account.rows[0].account_type;
//       // console.log("Booking Event insertion Data:", {
//       //   // bookingId: result.rows[0].id,
//       //   eventType: legacyScheduleData.Type === "Repeat" ? "REPEAT" : "ONE_TIME",
//       //   timestamp: legacyScheduleData?.Start || bookingDate,
//       //   createdFor: customerAccounType,
//       //   createdAt: new Date(),
//       //   updatedAt: new Date(),
//       // });
//       await client.query(
//         `INSERT INTO booking_event (
//         booking_id,
//         event_type,
//         timestamp,
//         created_for,
//         created_at,
//         updated_at
//       ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
//         [
//           result.rows[0].id, // booking_id
//           legacyScheduleData.Type === "Repeat" ? "REPEAT" : "ONE_TIME",
//           legacyScheduleData?.Start || bookingDate,
//           customerAccounType,
//           new Date(item.CreatedAt) ?? new Date(), // created_at
//           new Date(item.UpdatedAt) ?? new Date(), // updated_at
//         ]
//       );
//     }

//     await client.query("COMMIT");
//     // console.log(`Booking inserted with ID: ${result.rows[0].id}`);
//     return;
//   } catch (err: any) {
//     await client.query("ROLLBACK");
//     console.error("Error inserting booking:", err);
//     throw new Error(`Failed to insert booking: ${err?.message}`);
//   } finally {
//     client.release();
//   }
// }

export async function updateBooking(item: any, mysqlConn: any, id?: any) {
  const client = await pgPool.connect();

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
      throw new Error(`Booking with legacy_id ${item.Id} not found.`);
      // return;
    }

    const bookingId = existing.rows[0].id;

    const accountId = await getCustomerAccountIdByLegacyId(item.CustomerId);
    if (!accountId) {
      await client.query("ROLLBACK");
      console.log(`Account with legacy_id ${item.CustomerId} not found.`);
      throw new Error(`Account with legacy_id ${item.CustomerId} not found.`);
      // return;
    }

    const propertyId = await getPropertyIdByLegacyId(item.PropertyId);
    if (!propertyId) {
      await client.query("ROLLBACK");
      console.log(`Property with legacy_id ${item.PropertyId} not found.`);
      throw new Error(`Property with legacy_id ${item.PropertyId} not found.`);
      // return;
    }

    const unitId = await getUnitFromUnitResident(item.UnitResidentId);
    if (!unitId) {
      await client.query("ROLLBACK");
      console.log(`Unit with legacy_id ${item.UnitResidentId} not found.`);
      throw new Error(`Unit with legacy_id ${item.UnitResidentId} not found.`);
      // return;
    }

    const [rows]: any = await mysqlConn.execute(
      `SELECT service_line_option_id FROM service_line_option_mapping WHERE option_id = ?`,
      [item?.OptionId]
    );

    if (!rows || rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(`No service_line_option_id for option_id ${item.OptionId}`);
      throw new Error(
        `No service_line_option_id found for option_id ${item.OptionId}`
      );
      // return;
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
      throw new Error(`Service not found for legacy_id ${legacyServiceId}`);
      // return;
    }
    console.log(`booking start date: ${item.Start}`);

    const serviceId = serviceResult.rows[0].id;

    const bookingDate =
      item.Start === "0000-00-00 00:00:00.000000" || !item.Start
        ? "1970-01-01 00:00:00.000"
        : item.Start;

    await client.query(
      `UPDATE booking SET
        account_id = $1,
        property_id = $2,
        service_id = $3,
        date = $4,
        payment_method_id = $5,
        unit_id = $6,
        notes=$7,
        updated_at = $8,
        deleted_at = $9
      WHERE legacy_id = $10`,
      [
        accountId,
        propertyId,
        serviceId,
        bookingDate,
        item.PaymentTokenId ?? null,
        unitId,
        item.Notes ?? null,
        item.UpdateAt ?? new Date(),
        item.Deleted_at,
        item.Id,
      ]
    );

    if (item.Start && item.End) {
      // Assuming time_window has a booking_id reference
      const twResult = await client.query(
        `SELECT time_window_id FROM booking WHERE id = $1`,
        [bookingId]
      );

      if (twResult.rows.length > 0) {
        const timeWindowId = twResult.rows[0].time_window_id;

        const name = classifyTimeRange(item.Start, item.End);

        // const startTime =
        //   item.Start == "0000-00-00 00:00:00" || !item.Start
        //     ? "1970-01-01 00:00:00.000"
        //     : item.Start?.replace("+00:00", "");
        // const endTime =
        //   item.End === "0000-00-00 00:00:00" || !item.End
        //     ? "1970-01-01 00:00:00.000"
        //     : item.End?.replace("+00:00", "");

        const startTime = formatDateToSixDecimals(item.Start);
        const endTime = formatDateToSixDecimals(item.Start);
        await client.query(
          `UPDATE time_window SET
            start_time = $1,
            end_time = $2,
            name=$3,
            updated_at = $4
          WHERE id = $5`,
          [startTime, endTime, name, new Date(), timeWindowId]
        );

        console.log(`🕒 TimeWindow for booking ${item.Id} updated.`);
      }
    }

    if (item.Redo && Number(item.Redo) > 0) {
      // Delete specific statuses
      await client.query(
        `DELETE FROM status_history
         WHERE booking_id = $1
         AND status IN ('ON_THE_WAY', 'CLOCK_IN', 'COMPLETED', 'CANCELLED')`,
        [bookingId]
      );

      await client.query(
        `UPDATE booking SET
          redo_booking_id = $1,
          redo_note = $2
        WHERE id = $3`,
        [bookingId, item?.Note, bookingId]
      );
    }

    await client.query("COMMIT");
    console.log(`✅ Booking with legacy_id ${item.Id} updated successfully.`);
    return bookingId;
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("❌ Error updating booking:", err);
    throw new Error(`Failed to update booking: ${err?.message}`);
  } finally {
    client.release();
  }
}

export async function insertBookingAddOns(item: any, mysqlConn: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    const bookingId = await getBookingIdByLegacyId(item.BookingId);
    const addOnId = await getAddOnIdByLegacyId(item.AddOnId);

    if (!bookingId || !addOnId) {
      await client.query("ROLLBACK");
      throw new Error("No bookingId or addon Id found");
      // return;
    }

    // Check if the record already exists
    const existing = await client.query(
      `SELECT id FROM booking_addon WHERE booking_id = $1 AND addon_id = $2`,
      [bookingId, addOnId]
    );

    if (existing.rows.length > 0) {
      await client.query("ROLLBACK");
      throw new Error(
        `Booking add-on already exists for booking ${item.BookingId} and addon ${item.AddOnId}`
      );
      // return;
    }

    await client.query(
      `INSERT INTO booking_addon (
          booking_id, addon_id, legacy_id
        ) VALUES ($1, $2, $3)`,
      [bookingId, addOnId, item.Id]
    );

    await client.query("COMMIT");
    console.log("Booking add-on inserted successfully.");
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("Error inserting booking add-on:", err);
    throw new Error(`Failed to insert booking addons: ${err?.message}`);
  } finally {
    client.release();
  }
}

export async function updateBookingAddOn(item: any, mysqlConn: any, id?: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    const bookingId = await getBookingIdByLegacyId(item.BookingId);
    const addOnId = await getAddOnIdByLegacyId(item.AddOnId);

    if (!bookingId || !addOnId) {
      await client.query("ROLLBACK");
      throw new Error(
        `Missing booking or add-on reference for legacy ID: ${item.Id}`
      );
      // return;
    }

    // Check if the record exists
    const { rows: existingRows } = await client.query(
      `SELECT * FROM booking_addon WHERE legacy_id = $1`,
      [item.Id]
    );

    if (item.DeletedAt) {
      if (existingRows.length > 0) {
        await client.query(`DELETE FROM booking_addon WHERE legacy_id = $1`, [
          item.Id,
        ]);
        await client.query("COMMIT");
        console.log(`✅ Deleted booking_addon with legacy_id ${item.Id}`);
      } else {
        await client.query("ROLLBACK");
        console.warn(
          `⚠️ Tried to delete non-existing booking_addon with legacy_id ${item.Id}`
        );
      }
      return;
    }

    if (existingRows.length > 0) {
      await client.query(
        `UPDATE booking_addon
         SET booking_id = $1,
             addon_id = $2
         WHERE legacy_id = $3`,
        [bookingId, addOnId, item.Id]
      );
      console.log(`✅ Updated booking_addon (legacy_id: ${item.Id})`);
    } else {
      // Case 3: Record does not exist and not deleted => insert it
      await client.query(
        `INSERT INTO booking_addon (
           booking_id,
           addon_id,
           legacy_id
         ) VALUES ($1, $2, $3)`,
        [bookingId, addOnId, item.Id]
      );
      console.log(`✅ Inserted new booking_addon (legacy_id: ${item.Id})`);
    }

    await client.query("COMMIT");
    console.log(`Booking add-on (legacy_id: ${item.Id}) updated successfully.`);
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error(
      `Error updating booking add-on (legacy_id: ${item.Id}):`,
      err
    );
    throw new Error(`Failed to update booking addons: ${err?.message}`);
  } finally {
    client.release();
  }
}

export async function getBookingIdByLegacyId(legacyId: number) {
  const client = await pgPool.connect();

  try {
    console.log("Checking booking Legacy");
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
    console.info("🚀 ~ getBookingIdByLegacyId ~ info:");
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
export async function insertBookingActivity(item: any, mysqlConn: any) {
  let client = null;
  try {
    client = await pgPool.connect();
    const bookingId = await getBookingIdByLegacyId(item.BookingId);

    if (!bookingId) {
      await client.query("ROLLBACK");
      throw new Error(`Booking with legacy_id ${item.BookingId} not found.`);
    }

    const message = `${item.Time ?? ""} ${item.EventName ?? ""} ${
      item.Detail?.service_name ?? ""
    }`.trim();

    const bookingEventName = mapEventNameToBookingStatus(item.EventName);
    const { rows } = await client.query(
      `SELECT id FROM status_history WHERE legacy_id = $1 LIMIT 1`,
      [bookingId]
    );

    if (rows.length > 0) {
      // Update existing record
      await client.query(
        `UPDATE status_history 
         SET time = $1, platform = $2, message = $3, created_at = $4 
         WHERE id = $5`,
        [
          item.Time || null,
          item.Platform || null,
          message,
          new Date(item.CreatedAt) || new Date(),
          rows[0].id,
        ]
      );

      console.log(`🔁 Updated status history for booking ID ${bookingId}`);
    } else {
      // Insert new record
      await client.query(
        `INSERT INTO status_history (
            booking_id, status, time, platform, message, created_at, legacy_id
          ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [
          bookingId,
          bookingEventName || null,
          item.Time || null,
          item.Platform || null,
          message,
          new Date(item.CreatedAt) ?? new Date(),
          item.Id,
        ]
      );

      console.log(`✅ Inserted status history for booking ID ${bookingId}`);
    }
  } catch (err: any) {
    console.error("❌ Error inserting status history:", err);
    throw new Error(`Failed to insert booking Activity: ${err?.message}`);
  } finally {
    if (client) {
      client.release();
    }
  }
}

export async function insertRecurringScheduleItem(
  recurringData: any,
  mysqlConn: any
) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");
    const [rows]: any[] = await mysqlConn.execute(
      "SELECT Id, Interval, Frequency FROM recurrence WHERE Id = ?",
      [recurringData.RecurrenceId]
    );

    if (!rows || rows.length === 0) {
      await client.query("ROLLBACK");
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

    const bookingId = await getBookingIdByLegacyId(recurringData.BookingId);

    if (!bookingId) {
      await client.query("ROLLBACK");
      throw new Error(
        `booking with Legacy ID ${recurringData.BookingId} not found`
      );
    }
    await client.query(
      `UPDATE bookings SET repeat_interval = $1 WHERE id = $2`,
      [totalDays, bookingId]
    );

    await client.query("COMMIT");
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("Error inserting recurring schedule:", err);
    throw new Error(`Failed to insert Recurring Schedule: ${err?.message}`);
  } finally {
    client.release();
  }
}

//use this with insert repeatData
export async function insertRepeatBookings(
  repeatBookingData: any,
  mysqlConn: any,
  id?: any
) {
  const client = await pgPool.connect();
  try {
    const { new_data } = repeatBookingData;
    const { BookingId, OriginalBookingId } = JSON.parse(new_data);

    // const bookingIds: number[] = expandBookingRange(record_id);
    // const bookingsToUpdate = bookingIds.filter(
    //   (id) => id !== OriginalBookingId
    // );

    await client.query("BEGIN");

    // Get repeat_interval of original booking
    const { rows: originalRows } = await client.query(
      `SELECT id,repeat_interval FROM booking WHERE legacy_id = $1`,
      [OriginalBookingId]
    );

    if (!originalRows.length) {
      throw new Error(`Original booking ${OriginalBookingId} not found`);
    }

    const { rows: bookingRows } = await client.query(
      `SELECT id,repeat_interval FROM booking WHERE legacy_id = $1`,
      [BookingId]
    );

    if (!bookingRows.length) {
      throw new Error(`Original booking ${BookingId} not found`);
    }

    const originalBookingId = originalRows[0].id;
    const repeatInterval = originalRows[0].repeat_interval;

    const { rowCount } = await client.query(
      `UPDATE booking SET repeat_interval = $1, source_booking_id = $2
       WHERE legacy_id = $3`,
      [repeatInterval, originalBookingId, BookingId]
    );

    if (rowCount === 0) {
      throw new Error(`Booking with legacy_id ${BookingId} not found`);
    }

    // Update each repeated booking

    await client.query("COMMIT");
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("Error updating repeated bookings:", err);
    throw new Error(`Failed to update repeated bookings: ${err?.message}`);
  } finally {
    client.release();
  }
}

export async function insertBookingFeedback(feedbackData: any, mysqlConn: any) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    const bookingId = await getBookingIdByLegacyId(feedbackData.BookingId);
    const accountId = await getCustomerAccountIdByLegacyId(
      feedbackData.CustomerId
    );

    if (!bookingId) {
      await client.query("ROLLBACK");
      console.log(
        `Booking with legacy_id ${feedbackData.BookingId} not found.`
      );
      throw new Error(
        `Booking with legacy_id ${feedbackData.BookingId} not found.`
      );
      // return;
    }

    if (!accountId) {
      await client.query("ROLLBACK");
      console.log(
        `Booking with legacy_id ${feedbackData.CustomerId} not found.`
      );
      throw new Error(
        `Booking with legacy_id ${feedbackData.CustomerId} not found.`
      );
      // return;
    }

    const existing = await client.query(
      `SELECT id FROM booking_feedback WHERE booking_id = $1 AND account_id = $2`,
      [bookingId, accountId]
    );

    if (existing.rows.length > 0) {
      await client.query("ROLLBACK");
      throw new Error(
        `Feedback already exists for booking_id ${bookingId} and account_id ${accountId}.`
      );
    }

    await client.query(
      `INSERT INTO booking_feedback (
        booking_id, account_id, rating, comment, updated_at
      ) VALUES ($1, $2, $3, $4, $5)`,
      [
        bookingId,
        accountId,
        feedbackData.Rating,
        feedbackData.Feedback,
        feedbackData.UpdatedAt ?? new Date(),
      ]
    );

    await client.query("COMMIT");
    console.log("Booking feedback inserted successfully.");
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("Error inserting booking feedback:", err);
    throw new Error(`Failed to insert booking feedback: ${err?.message}`);
  } finally {
    client.release();
  }
}

export async function updateBookingFeedback(
  feedbackData: any,
  mysqlConn: any,
  id?: any
) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    const bookingId = await getBookingIdByLegacyId(feedbackData.BookingId);
    const accountId = await getCustomerAccountIdByLegacyId(
      feedbackData.CustomerId
    );

    if (!bookingId) {
      await client.query("ROLLBACK");
      console.log(
        `Booking with legacy_id ${feedbackData.BookingId} not found.`
      );
      // return;
      throw new Error(
        `Booking with legacy_id ${feedbackData.BookingId} not found.`
      );
    }

    if (!accountId) {
      await client.query("ROLLBACK");
      console.log(
        `Account with legacy_id ${feedbackData.CustomerId} not found.`
      );
      throw new Error(
        `Account with legacy_id ${feedbackData.CustomerId} not found.`
      );
      // return;
    }

    // Check if the feedback already exists
    const existing = await client.query(
      `SELECT id FROM booking_feedback WHERE booking_id = $1 AND account_id = $2`,
      [bookingId, accountId]
    );

    if (existing.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log("No existing feedback found for this booking and account.");
      // return;
      throw new Error(
        "No existing feedback found for this booking and account."
      );
    }

    // Perform the update
    await client.query(
      `UPDATE booking_feedback
       SET rating = $1, comment = $2, updated_at = $3, deleted_at =$4
       WHERE booking_id = $5 AND account_id = $6`,
      [
        feedbackData.Rating,
        feedbackData.Feedback,
        feedbackData.UpdatedAt ?? new Date(),
        feedbackData.DeletedAt ?? null,
        bookingId,
        accountId,
      ]
    );

    await client.query("COMMIT");
    console.log("✅ Booking feedback updated successfully.");
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("❌ Error updating booking feedback:", err);
    throw new Error(`Failed to update booking feedback: ${err?.message}`);
  } finally {
    client.release();
  }
}

export async function insertBookingServiceDetails(
  bookingData: any,
  mysqlConn: any
) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");

    const bookingId = await getBookingIdByLegacyId(bookingData.BookingId);
    if (!bookingId) {
      await client.query("ROLLBACK");
      console.error(`Booking not found for legacy ID ${bookingData.BookingId}`);
      // return;
      throw new Error(
        `Booking not found for legacy ID ${bookingData.BookingId}`
      );
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

    if (bookingData.ClockedIn) {
      const existing = await client.query(
        `SELECT id FROM status_history
   WHERE booking_id = $1 AND status = 'CLOCK_IN'
   LIMIT 1`,
        [bookingId]
      );

      const createdAt = bookingData.CreatedAt || new Date();

      if (existing.rows.length > 0) {
        await client.query(
          `UPDATE status_history
       SET time = $1,
           created_at = $2
       WHERE id = $3`,
          [bookingData.ClockedIn, createdAt, existing.rows[0].id]
        );
      } else {
        await client.query(
          `INSERT INTO status_history (booking_id, status, time, created_at, legacy_id)
       VALUES ($1, $2, $3, $4, $5)`,
          [
            bookingId,
            "CLOCK_IN",
            bookingData.ClockedIn,
            createdAt,
            bookingData.Id,
          ]
        );
      }
    }

    if (bookingData.OnTheWay) {
      const existing = await client.query(
        `SELECT id FROM status_history
   WHERE booking_id = $1 AND status = 'ON_THE_WAY'
   LIMIT 1`,
        [bookingId]
      );

      const createdAt = bookingData.CreatedAt || new Date();

      if (existing.rows.length > 0) {
        await client.query(
          `UPDATE status_history
           SET time = $1,
               created_at = $2
           WHERE id = $3`,
          [bookingData.OnTheWay, createdAt, existing.rows[0].id]
        );
      } else {
        await client.query(
          `INSERT INTO status_history (booking_id, status, time, created_at, legacy_id)
           VALUES ($1, $2, $3, $4,$5)`,
          [
            bookingId,
            "ON_THE_WAY",
            bookingData.OnTheWay,
            createdAt,
            bookingData.Id,
          ]
        );
      }
    }

    if (bookingData.ClockedOut) {
      const existing = await client.query(
        `SELECT id FROM status_history
   WHERE booking_id = $1 AND status = 'COMPLETED'
   LIMIT 1`,
        [bookingId]
      );

      const createdAt = bookingData.CreatedAt || new Date();

      if (existing.rows.length > 0) {
        await client.query(
          `UPDATE status_history
           SET time = $1,
               created_at = $2
           WHERE id = $3`,
          [bookingData.ClockedOut, createdAt, existing.rows[0].id]
        );
      } else {
        await client.query(
          `INSERT INTO status_history (booking_id, status, time, created_at, legacy_id)
           VALUES ($1, $2, $3, $4, $5)`,
          [
            bookingId,
            "COMPLETED",
            bookingData.ClockedOut,
            createdAt,
            bookingData.Id,
          ]
        );
      }
    }

    if (bookingData?.CanceledAt) {
      const cancelTime =
        bookingData.CanceledAt === "0000-00-00 00:00:00"
          ? new Date("1970-01-01T00:00:00.000Z")
          : new Date(bookingData.CanceledAt);

      // Upsert CANCELLED status into status_history
      const existing = await client.query(
        `SELECT id FROM status_history
   WHERE booking_id = $1 AND status = 'CANCELLED'
   LIMIT 1`,
        [bookingId]
      );

      if (existing.rows.length > 0) {
        await client.query(
          `UPDATE status_history
           SET time = $1,
               created_at = $2
           WHERE id = $3`,
          [cancelTime, cancelTime, existing.rows[0].id]
        );
      } else {
        await client.query(
          `INSERT INTO status_history (booking_id, status, time, created_at, legacy_id)
           VALUES ($1, $2, $3, $4, $5)`,
          [
            bookingId,
            "CANCELLED",
            cancelTime,
            bookingData.CreatedAt ?? new Date(),
            bookingData.Id,
          ]
        );
      }

      // Upsert booking_cancel_details
      await client.query(
        `INSERT INTO booking_cancel_details (booking_id, reasons, comment, created_at)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (booking_id)
         DO UPDATE SET
           reasons = EXCLUDED.reasons,
           comment = EXCLUDED.comment,
           created_at = EXCLUDED.created_at;`,
        [
          bookingId,
          bookingData.CancelationReason
            ? `{${bookingData.CancelationReason.replace(/'/g, "\\'")}}`
            : "{}",
          bookingData.CancelationNotes || null,
          cancelTime,
        ]
      );
    }

    await client.query("COMMIT");
    console.log(`Booking ID ${bookingId} updated from kes.`);
  } catch (error: any) {
    await client.query("ROLLBACK");
    console.error("Error updating booking from kes:", error);
    throw new Error(
      `Failed to update booking service details: ${error?.message}`
    );
  } finally {
    client.release();
  }
}

export async function updateBookingServiceDetails(
  bookingData: any,
  mysqlConn: any,
  id?: string
) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    // Get the booking ID from legacy ID
    const bookingId = await getBookingIdByLegacyId(bookingData.BookingId);
    if (!bookingId) {
      await client.query("ROLLBACK");
      console.error(`Booking not found for legacy ID ${bookingData.BookingId}`);
      // return;
      throw new Error(
        `Booking not found for legacy ID ${bookingData.BookingId}`
      );
    }

    // Determine booking status

    const status = mapLegacyStatusToBookingStatus(bookingData.Status);

    await client.query(
      `UPDATE booking
       SET status = $1,
           updated_at =$2
       WHERE id = $3`,
      [status, bookingData.UpdatedAt ?? new Date(), bookingId]
    );

    if (bookingData.ClockedIn) {
      const existing = await client.query(
        `SELECT id FROM status_history
   WHERE booking_id = $1 AND status = 'CLOCK_IN'
   LIMIT 1`,
        [bookingId]
      );

      const createdAt = bookingData.CreatedAt || new Date();

      if (existing.rows.length > 0) {
        await client.query(
          `UPDATE status_history
       SET time = $1,
           created_at = $2
       WHERE id = $3`,
          [bookingData.ClockedIn, createdAt, existing.rows[0].id]
        );
      } else {
        await client.query(
          `INSERT INTO status_history (booking_id, status, time, created_at, legacy_id)
       VALUES ($1, $2, $3, $4, $5)`,
          [
            bookingId,
            "CLOCK_IN",
            bookingData.ClockedIn,
            createdAt,
            bookingData.Id,
          ]
        );
      }
    }

    if (bookingData.OnTheWay) {
      const existing = await client.query(
        `SELECT id FROM status_history
   WHERE booking_id  = $1 AND status = 'ON_THE_WAY'
   LIMIT 1`,
        [bookingId]
      );

      const createdAt = bookingData.CreatedAt || new Date();

      if (existing.rows.length > 0) {
        await client.query(
          `UPDATE status_history
           SET time = $1,
               created_at = $2
           WHERE id = $3`,
          [bookingData.OnTheWay, createdAt, existing.rows[0].id]
        );
      } else {
        await client.query(
          `INSERT INTO status_history (booking_id, status, time, created_at,legacy_id)
           VALUES ($1, $2, $3, $4, $5)`,
          [
            bookingId,
            "ON_THE_WAY",
            bookingData.OnTheWay,
            createdAt,
            bookingData.Id,
          ]
        );
      }
    }

    if (bookingData.ClockedOut) {
      const existing = await client.query(
        `SELECT id FROM status_history
        WHERE booking_id  = $1 AND status = 'COMPLETED'
        LIMIT 1`,
        [bookingId]
      );

      const createdAt = bookingData.CreatedAt || new Date();

      if (existing.rows.length > 0) {
        await client.query(
          `UPDATE status_history
           SET time = $1,
               created_at = $2
           WHERE id = $3`,
          [bookingData.ClockedOut, createdAt, existing.rows[0].id]
        );
      } else {
        await client.query(
          `INSERT INTO status_history (booking_id, status, time, created_at, legacy_id)
           VALUES ($1, $2, $3, $4, $5)`,
          [
            bookingId,
            "COMPLETED",
            bookingData.ClockedOut,
            createdAt,
            bookingData.Id,
          ]
        );
      }
    }

    if (bookingData.CanceledAt) {
      const cancelTime =
        bookingData.CanceledAt === "0000-00-00 00:00:00"
          ? new Date("1970-01-01T00:00:00.000Z")
          : new Date(bookingData.CanceledAt);

      // Upsert CANCELLED status into status_history
      const existing = await client.query(
        `SELECT id FROM status_history
   WHERE booking_id  = $1 AND status = 'CANCELLED'
   LIMIT 1`,
        [bookingId]
      );

      if (existing.rows.length > 0) {
        await client.query(
          `UPDATE status_history
           SET time = $1,
               created_at = $2
           WHERE id = $3`,
          [cancelTime, cancelTime, existing.rows[0].id]
        );
      } else {
        await client.query(
          `INSERT INTO status_history (booking_id, status, time, created_at, legacy_id)
           VALUES ($1, $2, $3, $4, $5)`,
          [
            bookingId,
            "CANCELLED",
            cancelTime,
            bookingData.CreatedAt ?? new Date(),
            bookingData.Id,
          ]
        );
      }

      // Upsert booking_cancel_details
      await client.query(
        `INSERT INTO booking_cancel_details (booking_id, reasons, comment, created_at)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (booking_id)
         DO UPDATE SET
           reasons = EXCLUDED.reasons,
           comment = EXCLUDED.comment,
           created_at = EXCLUDED.created_at;`,
        [
          bookingId,
          bookingData.CancelationReason
            ? `{${bookingData.CancelationReason.replace(/'/g, "\\'")}}`
            : "{}",
          bookingData.CancelationNotes || null,
          cancelTime,
        ]
      );
    }

    if (bookingData.ServiceProviderManagerId) {
      // Step 1: Get management company ID from legacy ID

      const [managerRows]: any = await mysqlConn.query(
        `SELECT serviceProviderCompanyId FROM serviceprovidermanagers WHERE ServiceProviderManagerId = ?`,
        [bookingData.ServiceProviderManagerId]
      );

      const serviceProviderCompanyId =
        managerRows?.[0]?.serviceProviderCompanyId;

      const managementCompanyRes = await client.query(
        `SELECT id FROM service_provider_management_companies WHERE legacy_id = $1`,
        [serviceProviderCompanyId]
      );
      const managementCompanyId = managementCompanyRes.rows?.[0]?.id;

      // Step 2: Get company ID under management company
      let companyId = null;
      if (!managementCompanyId) {
        await client.query("ROLLBACK");
        throw new Error(
          `Management company not found for ServiceProviderManagerId ${bookingData.ServiceProviderManagerId}`
        );
      }
      const companyRes = await client.query(
        `SELECT id FROM company WHERE service_provider_management_company_id = $1 AND type = 'SERVICE_PROVIDER'`,
        [managementCompanyId]
      );
      companyId = companyRes.rows?.[0]?.id;

      if (!companyId) {
        await client.query("ROLLBACK");
        throw new Error(
          `Company not found for ServiceProviderManagerId ${bookingData.ServiceProviderManagerId}`
        );
      }

      // Step 3: Check for existing dispatch
      const dispatchRes = await client.query(
        `SELECT id FROM dispatch WHERE company_id = $1 AND booking_id = $2`,
        [companyId, bookingId]
      );

      const dispatchDate = bookingData.Start || bookingData.CreatedAt;
      const createdAt =
        !bookingData.CreatedAt ||
        bookingData.CreatedAt === "0000-00-00 00:00:00"
          ? new Date()
          : new Date(bookingData.CreatedAt);

      let dispatchId: string;
      if (dispatchRes.rows.length > 0) {
        dispatchId = dispatchRes.rows[0].id;
      } else {
        const { rows: existingDispatchRows } = await client.query(
          `SELECT id FROM dispatch WHERE booking_id = $1`,
          [bookingId]
        );

        if (existingDispatchRows.length > 0) {
          const existingDispatchId = existingDispatchRows[0].id;

          // Delete associated dispatch_pro entries
          await client.query(
            `DELETE FROM dispatch_pro WHERE dispatch_id = $1`,
            [existingDispatchId]
          );

          // Delete the dispatch itself
          await client.query(`DELETE FROM dispatch WHERE id = $1`, [
            existingDispatchId,
          ]);
        }
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
          `SELECT userId FROM serviceproviderrunners WHERE serviceProviderRunnerId = ?`,
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
              `UPDATE dispatch_pro
               SET deleted_at = NOW()
               WHERE dispatch_id = $1 AND deleted_at IS NULL`,
              [dispatchId]
            );

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

    await client.query("COMMIT");
    console.log(
      `Booking ID ${bookingId} service details updated successfully.`
    );
  } catch (error: any) {
    await client.query("ROLLBACK");
    console.error("Error updating booking service details:", error);
    throw new Error(
      `Failed to update booking service details: ${error?.message}`
    );
  } finally {
    client.release();
  }
}

export async function insertBookingTimeWindow(
  timeWindowData: any,
  mysqlConn: any
) {
  const client = await pgPool.connect();

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
      throw new Error(
        `Could not resolve service line data for option_id ${timeWindowData.OptionId}`
      );
      // return;
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
      throw new Error(
        `No PostgreSQL service_line found for legacy_id ${serviceLineId}`
      );
      // return;
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
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING id`,
      [
        timeWindowData.Id,
        timeWindowData.DayOfWeek,
        timeWindowData.StartTime, // need to change, look in update booking
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
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("❌ Error inserting time window:", err);
    throw new Error(`Failed to insert time window: ${err?.message}`);
  } finally {
    client.release();
  }
}

export async function updateBookingTimeWindow(
  timeWindowData: any,
  mysqlConn: any,
  id?: any
) {
  const client = await pgPool.connect();

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
      throw new Error(
        `Could not resolve service line data for option_id ${timeWindowData.OptionId}`
      );
      // return;
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
      throw new Error(
        `No PostgreSQL service_line found for legacy_id ${serviceLineId}`
      );
      // return;
    }

    const pgServiceLineId = pgServiceLineResult.rows[0].id;

    const startTime =
      timeWindowData.StartTime == "0000-00-00 00:00:00" ||
      !timeWindowData.StartTime
        ? "1970-01-01 00:00:00.000"
        : timeWindowData.StartTime?.replace("+00:00", "");
    const endTime =
      timeWindowData.EndTime === "0000-00-00 00:00:00" ||
      !timeWindowData.EndTime
        ? "1970-01-01 00:00:00.000"
        : timeWindowData.EndTime?.replace("+00:00", "");
    // Update the time_window in PostgreSQL
    await client.query(
      `UPDATE time_window
       SET name = $1,
           start_time = $2,
           end_time = $3,
           service_line_id = $4,
           updated_at = NOW(),
           deleted_at = $5
       WHERE legacy_id = $5`,
      [
        timeWindowData.DayOfWeek,
        startTime,
        endTime,
        pgServiceLineId,
        timeWindowData.Id,
        timeWindowData.deleted_at ?? null,
      ]
    );

    await client.query("COMMIT");
    console.log(`✅ Time window with legacy_id ${timeWindowData.Id} updated.`);
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("❌ Error updating time window:", err);
    throw new Error(`Failed to update time window: ${err?.message}`);
  } finally {
    client.release();
  }
}

export async function insertOneTimeScheduleWindow(
  scheduleData: any,
  mysqlConn: any
) {
  const client = await pgPool.connect();

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
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("❌ Error inserting one-time schedule:", err);
    throw new Error(`Failed to insert one-time schedule: ${err?.message}`);
  } finally {
    client.release();
  }
}

export async function updateOneTimeScheduleWindow(
  scheduleData: any,
  mysqlConn: any,
  id?: any
) {
  const client = await pgPool.connect();

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
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("❌ Error updating one-time schedule:", err);
    throw new Error(`Failed to update one-time schedule: ${err?.message}`);
  } finally {
    client.release();
  }
}

export async function insertBooking(bookingData: any, mysqlConn: any) {
  const client = await pgPool.connect();

  try {
    await client.query("BEGIN");

    // 1. First, get the marketId from marketservices using the MarketServiceId
    const [marketRows]: any = await mysqlConn.execute(
      `SELECT MarketId FROM marketservices WHERE Id = ? LIMIT 1`,
      [bookingData.MarketServiceId]
    );

    if (!marketRows || marketRows.length === 0) {
      await client.query("ROLLBACK");
      throw new Error(
        `Market not found for MarketServiceId: ${bookingData.MarketServiceId}`
      );
    }

    const marketId = marketRows[0].MarketId;

    // 1. Fetch the specific booking from MySQL with all related data
    const [bookingRows]: any = await mysqlConn.execute(
      `SELECT 
      spm.ServiceProviderCompanyId,
      spr.MobilePhone as spmMobilePhone,
      spr.Email as spmEmail,
      spr.Address as spmAddress,
      spr.City as spmCity,
      spr.State as spmState,
      spr.ZipCode as spmZipCode,
      bsd.ServiceProviderManagerId,
      bsd.ServiceProviderRunnerId,
      bsd.Status as bsdStatus,
      bsd.ClockedIn as bsdClockedIn,
      bsd.ClockedOut as bsdClockedOut,
      bsd.OnTheWay as bsdOnTheWay,
      bsd.CanceledAt as bsdCanceledAt,
      bsd.CancelationReason as bsdCancelationReason,
      bsd.CancelationNotes as bsdCancelationNotes,
      adon.Id as addonId,
      ba.Id as baId,
      adon.Name as addonName,
      adon.Description as addonDescription,
      adon.Price as addonPrice,
      adon.CreatedAt as addonCreatedAt,
      adon.DeletedAt as addonDeletedAt,
      ac.BelongsToMarket,
      b.*,
      b.Notes as bNotes,
      b.CreatedAt as bCreatedAt,
      u.FloorPlanId,
      slom.service_line_option_id,
      o.Title as oTitle,
      o.Description,
      fp.Id as fpId,
      fp.Baths as bathrooms,
      fp.Beds as bedrooms,
      fp.Name as floorPlanName,
      fp.CreatedAt as floorPlanCreatedAt,
      fp.DeletedAt as floorPlanDeletedAt,
      u.Id as unitID,
      u.FloorPlanId as unitFloorPlanId,
      u.ApartmentComplexId as unitApartmentComplexId,
      u.Building as building_number,
      u.Number as number,
      u.CreatedAt as uCreatedAt,
      ac.DataWarehouseId,
      c.*,
      c.AccountType as cAccountType,
      c.CreatedAt as cCreatedAt,
      c.UpdatedAt as cUpdatedAt,
      us.FirstName as userFirstName,
      us.LastName as userLastName,
      us.email as userEmail,
      us.Id as userId,
      us.password as UserPassword,
      us.CreatedAt as userCreatedAt,
      us.UpdatedAt as userUpdatedAt,
      us.DeletedAt as userDeletedAt,
      slo.service_line_id as sloServiceLineId,
      ms.Title as msTitle,
      ce.method as ceMethod,
      ce.code as ceCode,
      ce.details as ceDetails,
      ce.additional_notes as ceAdditional_notes,
      hbf.Description as HearAboutFrom
    FROM bookings b
    LEFT JOIN apartmentcomplexes ac ON b.PropertyId = ac.ApartmentId
    LEFT JOIN customers c ON b.CustomerId = c.CustomerId
    LEFT JOIN hearaboutfroms hbf ON hbf.Id = c.HearAboutFromId
    LEFT JOIN customer_entry ce ON ce.customer_id = c.CustomerId
    LEFT JOIN unitresidents ur ON c.CustomerId = ur.CustomerId
    LEFT JOIN units u ON ur.UnitId = u.Id
    LEFT JOIN floorplans fp ON fp.Id = u.FloorPlanId
    LEFT JOIN options o ON o.Id = b.OptionId
    LEFT JOIN service_line_option_mapping slom ON slom.option_id = b.OptionId
    LEFT JOIN service_line_options slo ON slo.id = slom.service_line_option_id
    LEFT JOIN bookingaddons ba ON ba.BookingId = b.Id
    LEFT JOIN addons adon ON adon.Id = ba.AddOnId
    LEFT JOIN marketservices ms ON ms.Id = o.MarketServiceId
    LEFT JOIN bookingservicedetails bsd ON bsd.BookingId = b.Id
    LEFT JOIN serviceprovidermanagers spm ON spm.ServiceProviderManagerId = bsd.ServiceProviderManagerId
    LEFT JOIN serviceproviderrunners spr ON spr.ServiceProviderRunnerId = bsd.ServiceProviderRunnerId
    LEFT JOIN users us ON us.Id = spr.UserId
    WHERE b.Id = ?
      AND b.DeletedAt IS NULL
      AND ur.DeletedAt IS NULL;
  `,
      [bookingData.Id]
    );

    if (!bookingRows || bookingRows.length === 0) {
      await client.query("ROLLBACK");
      throw new Error(
        `Booking not found with ID: ${bookingData.Id} and MarketServiceId: ${bookingData.MarketServiceId}`
      );
    }

    const item = bookingRows[0];

    let status = "IN_PROGRESS";
    if (item.bsdStatus === "Completed") {
      status = "COMPLETED";
    } else if (item.bsdStatus === "Canceled") {
      status = "CANCELLED";
    } else if (item.bsdStatus === "Pending") {
      status = "ASSIGNED";
    }

    // 2. Check if booking already exists by legacy_id
    const existing = await client.query(
      `SELECT id FROM booking WHERE legacy_id = $1`,
      [item.Id]
    );

    if (existing.rows.length > 0) {
      await client.query("ROLLBACK");
      return;
    }

    // 3. Get or create account
    const accountId = await getOrCreateAccount(item, client);
    if (!accountId) {
      await client.query("ROLLBACK");
      throw new Error(
        `Failed to process account for customer ${item.CustomerId}`
      );
    }

    // 4. Get or create property
    const propertyId = await getOrCreateProperty(item, client);
    if (!propertyId) {
      await client.query("ROLLBACK");
      throw new Error(`Failed to process property ${item.PropertyId}`);
    }

    // 5. Get or create floor plan
    const floorPlanId = await getOrCreateFloorPlan(item, propertyId, client);
    if (!floorPlanId) {
      await client.query("ROLLBACK");
      throw new Error(`Failed to process floor plan ${item.fpId}`);
    }

    // 6. Get or create unit
    const unitId = await getOrCreateUnit(item, propertyId, floorPlanId, client);
    if (!unitId) {
      await client.query("ROLLBACK");
      throw new Error(`Failed to process unit ${item.unitId}`);
    }

    // 7. Get or create service
    const serviceId = await getOrCreateService(item, client);
    if (!serviceId) {
      await client.query("ROLLBACK");
      throw new Error(`Failed to process service for option ${item.OptionId}`);
    }

    // 8. Create the booking
    const bookingDate = item.Start ?? item.CreatedAt;
    const result = await client.query(
      `INSERT INTO booking (
        reference_number,
        account_id,
        property_id,
        service_id,
        date,
        status,
        notes,
        payment_method_id,
        unit_id,
        legacy_id,
        created_at,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      RETURNING id`,
      [
        `BK-${item.Id}`,
        accountId,
        propertyId,
        serviceId,
        bookingDate,
        // Default status
        status,
        item.Notes || null,
        item.PaymentTokenId || null,
        unitId,
        item.Id,
        item.bCreatedAt ? new Date(item.bCreatedAt) : new Date(),
        new Date(),
      ]
    );

    const newBookingId = result.rows[0].id;

    // 9. Create time window
    await createTimeWindow(item, newBookingId, client);

    // 10. Process schedule if exists
    if (item.ScheduleId) {
      await processSchedule(item, newBookingId, mysqlConn, client);
    }

    if (item.addonId) {
      await processAddon(item, newBookingId, serviceId, client);
    }
    if (item.bsdClockedIn) {
      await insertBookingStatusHistory(
        newBookingId,
        "CLOCK_IN",
        item.bsdClockedIn,
        client,
        item.bCreatedAt ?? new Date()
      );
    }

    if (item.bsdOnTheWay) {
      await insertBookingStatusHistory(
        newBookingId,
        "ON_THE_WAY",
        item.bsdOnTheWay,
        client,
        item.bCreatedAt ?? new Date()
      );
    }

    if (item.bsdClockedOut) {
      await insertBookingStatusHistory(
        newBookingId,
        "COMPLETED",
        item.bsdClockedOut,
        client,
        item.bCreatedAt ?? new Date()
      );
    }

    if (item.bsdCanceledAt) {
      await insertBookingStatusHistory(
        newBookingId,
        "CANCELLED",
        item.bsdCanceledAt,
        client,
        item.bCreatedAt ?? new Date()
      );
    }

    await client.query("COMMIT");
    return newBookingId;
  } catch (err: any) {
    await client.query("ROLLBACK");
    console.error("Error inserting booking:", err);
    throw new Error(`Failed to insert booking: ${err.message}`);
  } finally {
    client.release();
  }
}
// Helper functions
async function getOrCreateAccount(item: any, client: any): Promise<number> {
  // Check if account exists
  const accountType =
    item.cAccountType === "SHELL_ACCOUNT" ? "STR" : "RESIDENT";

  let username =
    accountType === "STR" ? `${item.CustomerId}-${item.Email}` : item.Email;

  const accountResult = await client.query(
    `SELECT id FROM account WHERE legacy_id = $1 and account_type in ('RESIDENT','STR')`,
    [item.CustomerId]
  );

  if (accountResult.rows.length > 0) {
    return accountResult.rows[0].id;
  }

  if (accountType === "RESIDENT") {
    const usernameCheck = await client.query(
      `SELECT id FROM account WHERE username = $1 AND account_type = 'RESIDENT'`,
      [item.Email]
    );
    if (usernameCheck.rows.length > 0) {
      username = `${item.CustomerId}-duplicate${item.Email}`;
    }
  }
  // Create new account

  // 1. First create the account
  const accountInsertResult = await client.query(
    `INSERT INTO account (
      first_name, last_name, email, reference_id, username,
      phone, status, zip_code, account_type, legacy_id,
      stripe_customer_id, created_at, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    RETURNING id`,
    [
      item.FirstName,
      item.LastName || null,
      item.Email,
      accountType !== "STR" ? `RN-${item.CustomerId}` : null,
      username,
      item.MobilePhone,
      "ACTIVE",
      item.ZipCode,
      accountType,
      item.CustomerId,
      item.ExternalId,
      item.CreatedAt ? new Date(item.CreatedAt) : new Date(),
      new Date(),
    ]
  );

  const accountId = accountInsertResult.rows[0].id;

  // 2. Create account details
  await client.query(
    `INSERT INTO account_details (
      account_id, address, city, state,
      created_at, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6)`,
    [
      accountId,
      item.BillingAddress || null,
      item.City || null,
      item.State || null,
      item.CreatedAt ? new Date(item.CreatedAt) : new Date(),
      new Date(),
    ]
  );

  // 3. Create home access instructions if data exists
  if (
    item.ceMethod ||
    item.ceCode ||
    item.ceDetails ||
    item.ceAdditional_notes
  ) {
    await client.query(
      `INSERT INTO home_access_instruction (
        account_id, entry, entry_code, details, notes,
        created_at, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [
        accountId,
        item.ceMethod || "Other",
        item.ceCode || null,
        item.ceDetails || null,
        item.ceAdditional_notes || null,
        item.CreatedAt ? new Date(item.CreatedAt) : new Date(),
        new Date(),
      ]
    );
  }

  // 4. Create account credentials if password exists
  if (item.Password) {
    const hashedPassword = await bcrypt.hash(item.Password, 10);
    const authInfo = JSON.stringify({
      hasher: "bcrypt",
      password: hashedPassword,
    });

    await client.query(
      `INSERT INTO account_credential (
        account_id, provider, provider_key, auth_info,
        created_at, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        accountId,
        "PASSWORD",
        item.Email,
        authInfo,
        item.CreatedAt ? new Date(item.CreatedAt) : new Date(),
        new Date(),
      ]
    );
  }

  return accountId;
}

async function getOrCreateProperty(item: any, client: any) {
  const propertyResult = await client.query(
    `SELECT id FROM property WHERE legacy_id = $1`,
    [item.PropertyId]
  );

  if (propertyResult.rows.length > 0) {
    return propertyResult.rows[0].id;
  }

  // In a real scenario, you would create the property here
  throw new Error(`Property with legacy_id ${item.PropertyId} not found`);
}

async function getOrCreateFloorPlan(
  item: any,
  propertyId: number,
  client: any
): Promise<number> {
  const floorPlanResult = await client.query(
    `SELECT id FROM floor_plan WHERE legacy_id = $1`,
    [item.fpId]
  );

  if (floorPlanResult.rows.length > 0) {
    return floorPlanResult.rows[0].id;
  }

  const result = await client.query(
    `INSERT INTO floor_plan (
      name, description, bedrooms, bathrooms, legacy_id,
      property_id, created_at, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    RETURNING id`,
    [
      item.floorPlanName,
      item.floorPlanName,
      item.bedrooms,
      item.bathrooms,
      item.fpId,
      propertyId,
      item.CreatedAt ?? new Date(),
      new Date(),
    ]
  );

  return result.rows[0].id;
}

async function getOrCreateUnit(
  item: any,
  propertyId: number,
  floorPlanId: number,
  client: any
): Promise<number> {
  const unitResult = await client.query(
    `SELECT id FROM unit WHERE legacy_id = $1`,
    [item.unitId]
  );

  if (unitResult.rows.length > 0) {
    return unitResult.rows[0].id;
  }

  const result = await client.query(
    `INSERT INTO unit (
      property_id, floor_plan_id, number, building_number,
      legacy_id, created_at, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
    RETURNING id`,
    [
      propertyId,
      floorPlanId,
      item.unit_number,
      item.building_number,
      item.unitID,
      item.CreatedAt ?? new Date(),
      new Date(),
    ]
  );

  return result.rows[0].id;
}

async function getOrCreateService(item: any, client: any) {
  // First try to get service_line_option_id from mapping
  const serviceLineUuidId = getServiceLineUuid(item.msTitle);

  if (!serviceLineUuidId) {
    return null;
  }

  // Check if service exists
  const serviceResult = await client.query(
    `SELECT id FROM service WHERE name = $1 AND description = $2 AND service_line_id = $3`,
    [item.oTitle, item.Description, serviceLineUuidId]
  );

  if (serviceResult.rows.length > 0) {
    return serviceResult.rows[0].id;
  }

  // Create new service
  const result = await client.query(
    `INSERT INTO service (
      name, description, service_line_id, created_at, updated_at
    ) VALUES ($1, $2, $3, $4, $5)
    RETURNING id`,
    [
      item.oTitle,
      item.Description,
      serviceLineUuidId,
      item.CreatedAt ? new Date(item.CreatedAt) : new Date(),
      new Date(),
    ]
  );

  return result.rows[0].id;
}

async function createTimeWindow(item: any, bookingId: number, client: any) {
  const timeRangeName = classifyTimeRange(item.Start, item.End);

  const existing = await client.query(
    `SELECT id FROM time_window WHERE legacy_id = $1`,
    [item.Id]
  );

  if (existing.rows.length > 0) {
    console.log(
      `⚠️ Time window with legacy_id ${item.Id} already exists. Skipping insert.`
    );
    return existing.rows[0].id; // Return existing ID if needed
  }

  // const startTime =
  //   item.Start == "0000-00-00 00:00:00" || !item.Start
  //     ? "1970-01-01 00:00:00.000"
  //     : item.Start?.toString().replace("+00:00", "");
  // const endTime =
  //   item.End === "0000-00-00 00:00:00" || !item.End
  //     ? "1970-01-01 00:00:00.000"
  //     : item.End?.toString().replace("+00:00", "");

  const startTime = formatDateToSixDecimals(item.Start);
  const endTime = formatDateToSixDecimals(item.Start);

  const result = await client.query(
    `INSERT INTO time_window (
      name, start_time, end_time, legacy_id, service_line_id,
      created_at, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
    RETURNING id`,
    [
      timeRangeName,
      startTime,
      endTime,
      item.Id,
      getServiceLineUuid(item.msTitle),
      item.CreatedAt ? new Date(item.CreatedAt) : new Date(),
      item.UpdatedAt ? new Date(item.CreatedAt) : new Date(),
    ]
  );

  // Update booking with time window
  await client.query("UPDATE booking SET time_window_id = $1 WHERE id = $2", [
    result.rows[0].id,
    bookingId,
  ]);
}

async function processSchedule(
  item: any,
  bookingId: number,
  mysqlConn: any,
  client: any
) {
  const [scheduleRows]: any = await mysqlConn.execute(
    `SELECT * FROM schedules WHERE id = ? AND DeletedAt IS NULL`,
    [item.ScheduleId]
  );

  if (!scheduleRows || scheduleRows.length === 0) {
    console.log(`No schedule data found for id ${item.ScheduleId}`);
    return;
  }

  const scheduleData = scheduleRows[0];
  const accountResult = await client.query(
    `SELECT account_type FROM account WHERE id = $1`,
    [bookingId]
  );

  if (accountResult.rows.length === 0) return;

  await client.query(
    `INSERT INTO booking_event (
      booking_id,
      event_type,
      timestamp,
      created_for,
      created_at,
      updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6)`,
    [
      bookingId,
      scheduleData.Type === "Repeat" ? "REPEAT" : "ONE_TIME",
      scheduleData.Start ? new Date(scheduleData.Start) : new Date(),
      accountResult.rows[0].account_type,
      item.CreatedAt ? new Date(item.CreatedAt) : new Date(),
      item.UpdatedAt ? new Date(item.UpdatedAt) : new Date(),
    ]
  );
}

async function processAddon(
  item: any,
  bookingId: string,
  serviceId: string,
  client: any
) {
  try {
    // First get the service to get the service_line_id
    const serviceResult = await client.query(
      `SELECT service_line_id FROM service WHERE id = $1`,
      [serviceId]
    );

    if (serviceResult.rows.length === 0) {
      throw new Error(`Service not found with ID: ${serviceId}`);
    }

    const serviceLineId = serviceResult.rows[0].service_line_id;

    // Check if addon exists
    const addonResult = await client.query(
      `SELECT * FROM addon WHERE legacy_id = $1`,
      [item.addonId]
    );

    let addonId: string;
    const createdAt = item.addonCreatedAt
      ? new Date(item.addonCreatedAt)
      : new Date();
    const deletedAt =
      item.addonDeletedAt && item.addonDeletedAt !== "0000-00-00 00:00:00"
        ? new Date(item.addonDeletedAt)
        : null;

    if (addonResult.rows.length === 0) {
      // Create new addon
      const addonInsert = await client.query(
        `INSERT INTO addon (
          name, description, service_line_id, legacy_id,
          created_at, updated_at, deleted_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id`,
        [
          item.addonName,
          item.addonDescription,
          serviceLineId,
          item.addonId,
          createdAt,
          new Date(),
          deletedAt,
        ]
      );
      addonId = addonInsert.rows[0].id;

      // Create addon pricing
      await client.query(
        `INSERT INTO addon_pricing (
          addon_id, service_line_id, price, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5)`,
        [addonId, serviceLineId, item.addonPrice, createdAt, new Date()]
      );
    } else {
      addonId = addonResult.rows[0].id;
    }

    // Check if booking_addon relationship exists
    const bookingAddonResult = await client.query(
      `SELECT * FROM booking_addon 
       WHERE addon_id = $1 AND booking_id = $2`,
      [addonId, bookingId]
    );

    if (bookingAddonResult.rows.length === 0) {
      // Create booking_addon relationship
      await client.query(
        `INSERT INTO booking_addon (
          addon_id, booking_id, legacy_id
        ) VALUES ($1, $2, $3)`,
        [addonId, bookingId, item.baId]
      );
    }
  } catch (error) {
    console.error("Error processing addon:", error);
    throw error;
  }
}

async function insertBookingStatusHistory(
  bookingId: number,
  status: string,
  time: any,
  client: any,
  createdAt: any
) {
  if (!time || time === "0000-00-00 00:00:00") return;

  // Check if record exists
  const { rows } = await client.query(
    `SELECT id FROM status_history WHERE booking_id = $1 AND status = $2`,
    [bookingId, status]
  );

  if (rows.length > 0) {
    // Update existing
    await client.query(
      `UPDATE status_history SET time = $1, created_at = $2 WHERE booking_id = $3 AND status = $4`,
      [time, createdAt, bookingId, status]
    );
  } else {
    // Insert new
    await client.query(
      `INSERT INTO status_history (booking_id, status, time, created_at)
       VALUES ($1, $2, $3, $4)`,
      [bookingId, status, time, createdAt]
    );
  }
}
