import { BookingStatus, LegacyStatus } from "../constants/constant";

export type Recurrence = {
  Id: number;
  Interval: "Week";
  Frequency: number;
};

export function createSlug(name: string) {
  return name
    .toLowerCase()
    .replace(/ /g, "-") // Replace spaces with hyphens
    .replace(/[^\w-]+/g, "") // Remove all non-word characters
    .replace(/--+/g, "-") // Replace multiple hyphens with a single hyphen
    .replace(/^-+/, "") // Trim hyphens from the start
    .replace(/-+$/, ""); // Trim hyphens from the end
}

export function mapEventNameToBookingStatus(eventName: string): string | null {
  const map: Record<string, string> = {
    booking_addons_added: "BOOKING_ADDONS_ADDED",
    booking_addons_removed: "BOOKING_ADDONS_REMOVED",
    booking_canceled: "CANCELLED",
    booking_completed: "COMPLETED",
    booking_created: "BOOKING_CREATED",
    booking_late_addons_added: "BOOKING_LATE_ADDONS_ADDED",
    booking_late_addons_removed: "BOOKING_LATE_ADDONS_REMOVED",
    booking_notes_updated: "BOOKING_NOTES_UPDATED",
    booking_redo: "BOOKING_REDO",
    booking_rescheduled: "BOOKING_RESCHEDULED",
    booking_series_canceled: "BOOKING_SERIES_CANCELED",
    booking_series_rescheduled: "BOOKING_SERIES_RESCHEDULED",
    booking_started: "BOOKING_STARTED",
    booking_upgraded: "BOOKING_UPGRADED",
    feedback_submitted: "FEEDBACK_SUBMITTED",
    invoice_charged: "INVOICE_CHARGED",
    invoice_refunded: "INVOICE_REFUNDED",
    payment_failed: "PAYMENT_FAILED",
    payment_success: "PAYMENT_SUCCESS",
    service_pro_assigned: "ASSIGNED",
    service_pro_unassigned: "UNASSIGNED",
    service_provider_booking_approved: "SERVICE_PROVIDER_BOOKING_APPROVED",
    service_provider_booking_denied: "SERVICE_PROVIDER_BOOKING_DENIED",
    timeclock_edited: "TIMECLOCK_EDITED",
  };

  return map[eventName] ?? null;
}

export function getTotalDaysFromRecurrence(recurrence: Recurrence): number {
  const intervalToDaysMap: Record<string, number> = {
    Week: 7,
    // Add other intervals like 'Day', 'Month' if needed in future
  };

  const interval = recurrence.Interval;
  const frequency = recurrence.Frequency;

  const daysPerUnit = intervalToDaysMap[interval];
  if (!daysPerUnit) {
    throw new Error(`Unsupported interval: ${interval}`);
  }

  return frequency * daysPerUnit;
}

export function expandBookingRange(range: string): number[] {
  const [startStr, endStr] = range.split("-");
  const start = parseInt(startStr, 10);
  const end = parseInt(endStr, 10);

  if (isNaN(start) || isNaN(end) || start > end) {
    throw new Error(`Invalid range: ${range}`);
  }

  const result: number[] = [];
  for (let i = start; i <= end; i++) {
    result.push(i);
  }

  return result;
}

export function mapLegacyStatusToBookingStatus(status: string): BookingStatus {
  let bookingStatus: BookingStatus | null = null;

  if (status === LegacyStatus.Pending) {
    bookingStatus = BookingStatus.Assigned;
  }

  if (status === LegacyStatus.Canceled) {
    bookingStatus = BookingStatus.Cancelled;
  }

  if (status === LegacyStatus.Completed) {
    bookingStatus = BookingStatus.Completed;
  }

  return bookingStatus ?? BookingStatus.InProgress;
}
