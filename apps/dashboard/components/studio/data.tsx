"use client";

import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from "@tanstack/react-table";
import { cn } from "@/lib/cn";

/**
 * Studio-style data grid built on TanStack Table
 * (same table engine Supabase Studio uses for its data grids).
 */
export function DataTable<T extends object>({
  columns,
  rows,
  empty = "No rows",
  dense,
}: {
  columns: ColumnDef<T, unknown>[];
  rows: T[];
  empty?: string;
  dense?: boolean;
}) {
  const table = useReactTable({
    data: rows,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });
  const headerGroups = table.getHeaderGroups();
  const bodyRows = table.getRowModel().rows;
  return (
    <div className="overflow-auto rounded-md border border-border-muted">
      <table className="w-full border-collapse text-[13px]">
        <thead>
          {headerGroups.map((hg) => (
            <tr key={hg.id} className="bg-surface-300 text-left">
              {hg.headers.map((h) => (
                <th
                  key={h.id}
                  className={cn(
                    "border-b border-border-muted font-medium text-foreground-lighter",
                    dense ? "px-2 py-1.5" : "px-3 py-2",
                  )}
                >
                  {h.isPlaceholder
                    ? null
                    : flexRender(h.column.columnDef.header, h.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {bodyRows.length === 0 ? (
            <tr>
              <td
                colSpan={columns.length}
                className="px-3 py-6 text-center text-xs text-foreground-muted"
              >
                {empty}
              </td>
            </tr>
          ) : (
            bodyRows.map((row) => (
              <tr
                key={row.id}
                className="border-b border-border-muted last:border-0 hover:bg-surface-300/60"
              >
                {row.getVisibleCells().map((cell) => (
                  <td
                    key={cell.id}
                    className={cn(
                      "text-foreground-light",
                      dense ? "px-2 py-1.5" : "px-3 py-2",
                    )}
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

/** Render a small key/value cell, JSON-encoding non-strings. */
export function cell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
