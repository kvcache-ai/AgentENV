/** Formatting helpers shared by the dashboard and nodes views. */

const BYTE_UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"] as const;

export function formatBytes(bytes?: number, fractionDigits = 1): string {
  if (typeof bytes !== "number" || !Number.isFinite(bytes)) {
    return "—";
  }
  if (bytes < 1024) {
    return `${Math.round(bytes)} B`;
  }
  let value = bytes;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < BYTE_UNITS.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  const digits = value >= 100 ? 0 : fractionDigits;
  return `${value.toFixed(digits)} ${BYTE_UNITS[unitIndex]}`;
}

export function formatMegabytes(megabytes?: number): string {
  if (typeof megabytes !== "number" || !Number.isFinite(megabytes)) {
    return "—";
  }
  return formatBytes(megabytes * 1024 * 1024);
}

export function formatCount(value?: number): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "—";
  }
  return new Intl.NumberFormat("en-US").format(value);
}

export function formatPercent(value?: number | null, digits = 0): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "—";
  }
  return `${value.toFixed(digits)}%`;
}

export function percentOf(used?: number, total?: number): number | null {
  if (
    typeof used !== "number" ||
    typeof total !== "number" ||
    !Number.isFinite(used) ||
    !Number.isFinite(total) ||
    total <= 0
  ) {
    return null;
  }
  return (used / total) * 100;
}

export function formatDuration(ms: number): string {
  const abs = Math.abs(ms);
  const seconds = Math.round(abs / 1000);
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    const rest = seconds % 60;
    return rest === 0 ? `${minutes}m` : `${minutes}m ${rest}s`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    const rest = minutes % 60;
    return rest === 0 ? `${hours}h` : `${hours}h ${rest}m`;
  }
  const days = Math.floor(hours / 24);
  const rest = hours % 24;
  return rest === 0 ? `${days}d` : `${days}d ${rest}h`;
}

export function truncateId(id?: string, head = 12): string {
  if (!id) {
    return "—";
  }
  return id.length <= head + 3 ? id : `${id.slice(0, head)}…`;
}

export function shortCommit(commit?: string): string {
  if (!commit) {
    return "—";
  }
  return commit.length > 7 ? commit.slice(0, 7) : commit;
}
