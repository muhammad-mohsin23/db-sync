export const AdminId = "a36811b8-3e2e-4074-b1a2-99fc9006d6f3";

export const daysOfWeek: { [key: string]: number } = {
  Sunday: 0,
  Monday: 1,
  Tuesday: 2,
  Wednesday: 3,
  Thursday: 4,
  Friday: 5,
  Saturday: 6,
};

export enum LegacyStatus {
  Pending = "Pending",
  Canceled = "Canceled",
  Completed = "Completed",
  InProgress = "InProgress", // optional if used
}

// internal booking status in your system
export enum BookingStatus {
  Assigned = "ASSIGNED",
  Cancelled = "CANCELLED",
  Completed = "COMPLETED",
  InProgress = "IN_PROGRESS",
}
