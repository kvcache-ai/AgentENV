"use client";

import { useEffect, useState, useSyncExternalStore } from "react";

import { cn } from "@/lib/utils";
import { formatDuration } from "@/components/dashboard/format";

function parse(value?: string): Date | null {
  if (!value) {
    return null;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

const noopSubscribe = () => () => {};

function useIsHydrated(): boolean {
  return useSyncExternalStore(
    noopSubscribe,
    () => true,
    () => false,
  );
}

export function LocalTime({
  value,
  className,
  dateStyle,
  timeStyle,
}: {
  value?: string;
  className?: string;
  dateStyle?: Intl.DateTimeFormatOptions["dateStyle"];
  timeStyle?: Intl.DateTimeFormatOptions["timeStyle"];
}) {
  const hydrated = useIsHydrated();
  const date = parse(value);

  if (!date) {
    return <span className={cn("text-muted-foreground", className)}>—</span>;
  }

  const options: Intl.DateTimeFormatOptions =
    dateStyle === undefined && timeStyle === undefined
      ? { dateStyle: "medium", timeStyle: "medium" }
      : { dateStyle, timeStyle };

  const text = hydrated
    ? new Intl.DateTimeFormat(undefined, options).format(date)
    : (value ?? "—");

  return (
    <time
      dateTime={date.toISOString()}
      title={value}
      className={cn("tabular-nums", className)}
      suppressHydrationWarning
    >
      {text}
    </time>
  );
}

export function RelativeTime({
  value,
  className,
  refreshMs = 15_000,
}: {
  value?: string;
  className?: string;
  refreshMs?: number;
}) {
  const [now, setNow] = useState<number | null>(null);

  useEffect(() => {
    const tick = () => setNow(Date.now());
    const initial = setTimeout(tick, 0);
    const timer = setInterval(tick, refreshMs);
    return () => {
      clearTimeout(initial);
      clearInterval(timer);
    };
  }, [refreshMs]);

  const date = parse(value);
  if (!date) {
    return <span className={cn("text-muted-foreground", className)}>—</span>;
  }

  const delta = now === null ? null : date.getTime() - now;
  const label =
    delta === null
      ? "—"
      : delta >= 0
        ? `in ${formatDuration(delta)}`
        : `${formatDuration(delta)} ago`;

  return (
    <span
      title={value}
      className={cn("tabular-nums", className)}
      suppressHydrationWarning
    >
      {label}
    </span>
  );
}
