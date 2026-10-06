import type { SupabaseClient } from '@supabase/supabase-js';

/** Fluent PostgREST boundary: records contracts without contacting any service. */
export type DbCall = {
  table: string;
  operation: 'select' | 'upsert' | 'update' | 'insert';
  columns?: string;
  rows?: unknown;
  options?: Record<string, unknown>;
  filters: { column: string; value: unknown }[];
  single?: boolean;
};

export type DbResult = {
  data?: unknown;
  error?: { message: string } | null;
  count?: number | null;
};

export function stubClient(
  respond: (call: DbCall) => DbResult | Promise<DbResult> = () => ({ error: null }),
): { client: SupabaseClient; calls: DbCall[] } {
  const calls: DbCall[] = [];
  const client = {
    from(table: string) {
      const call: DbCall = { table, operation: 'select', filters: [] };
      const query = {
        select(columns: string) {
          call.columns = columns;
          return query;
        },
        eq(column: string, value: unknown) {
          call.filters.push({ column, value });
          return query;
        },
        in(column: string, value: unknown[]) {
          call.filters.push({ column, value });
          return query;
        },
        maybeSingle() {
          call.single = true;
          return query;
        },
        upsert(rows: unknown, options: Record<string, unknown>) {
          call.operation = 'upsert';
          call.rows = rows;
          call.options = options;
          return query;
        },
        update(rows: unknown, options: Record<string, unknown>) {
          call.operation = 'update';
          call.rows = rows;
          call.options = options;
          return query;
        },
        insert(rows: unknown) {
          call.operation = 'insert';
          call.rows = rows;
          return query;
        },
        then<TResult1 = DbResult, TResult2 = never>(
          resolve?: ((value: DbResult) => TResult1 | PromiseLike<TResult1>) | null,
          reject?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
        ): Promise<TResult1 | TResult2> {
          calls.push(call);
          return Promise.resolve().then(() => respond(call)).then(resolve, reject);
        },
      };
      return query;
    },
  };
  return { client: client as unknown as SupabaseClient, calls };
}

export async function captureErrors(run: () => Promise<void>): Promise<string[]> {
  const lines: string[] = [];
  const original = console.error;
  console.error = (...args: unknown[]) => lines.push(args.map(String).join(' '));
  try {
    await run();
  } finally {
    console.error = original;
  }
  return lines;
}
