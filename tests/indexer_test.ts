import { assert, assertEquals } from '@std/assert';
import { TASK_SECRET_HEADER } from '../supabase/functions/_shared/auth.ts';
import { IndexerDb } from '../supabase/functions/_shared/db.ts';
import { deriveGroupState, NO_FACTS } from '../supabase/functions/_shared/state.ts';
import type { EventPageStart, RpcEvent } from '../supabase/functions/_shared/stellar.ts';
import { captureErrors, type DbCall, type DbResult, stubClient } from './db_stub.ts';
import { allEvents, FACTORY_ID, factoryEvents, GROUP_ID, symbolTopic } from './fixture.ts';

// Import the real entrypoint without starting its Edge Function server. Restore
// the registration function even when importing fails; no network is involved.
const entry = await (async () => {
  const serve = Deno.serve;
  Deno.serve = (() => undefined) as unknown as typeof Deno.serve;
  try {
    return await import('../supabase/functions/indexer/index.ts');
  } finally {
    Deno.serve = serve;
  }
})();

const FIRST_LEDGER = Math.min(...allEvents.map((event) => event.ledger));
const LAST_LEDGER = Math.max(...allEvents.map((event) => event.ledger));
const SECRET = 'offline-test-invocation-secret-32-characters';
const TOKEN = `C${'A'.repeat(55)}`;
const configuration: Record<string, string> = {
  SUPABASE_URL: 'https://unused.invalid',
  SUPABASE_SERVICE_ROLE_KEY: 'offline-test-only',
  INDEXER_TASK_SECRET: SECRET,
  STELLAR_RPC_URL: 'https://unused.invalid/rpc',
  STELLAR_NETWORK_PASSPHRASE: 'Test SDF Network ; September 2015',
  STELLAR_NETWORK: 'testnet',
  FACTORY_CONTRACT_ID: FACTORY_ID,
  USDC_CONTRACT_ID: TOKEN,
  INDEXER_START_LEDGER: String(FIRST_LEDGER),
  INDEXER_MAX_LEDGER_RANGE: '100000',
};

async function withConfig(
  run: () => Promise<void>,
  overrides: Record<string, string | undefined> = {},
) {
  const values = { ...configuration, ...overrides };
  const previous = new Map(Object.keys(values).map((key) => [key, Deno.env.get(key)]));
  try {
    for (const [key, value] of Object.entries(values)) {
      if (value === undefined) Deno.env.delete(key);
      else Deno.env.set(key, value);
    }
    await run();
  } finally {
    for (const [key, value] of previous) {
      if (value === undefined) Deno.env.delete(key);
      else Deno.env.set(key, value);
    }
  }
}

function invocation(secret = SECRET) {
  return new Request('https://unused.invalid/indexer', {
    method: 'POST',
    headers: { [TASK_SECRET_HEADER]: secret },
  });
}

/** In-memory facts only; deliberately does not claim to model Postgres constraints. */
function harness(options: {
  checkpoint?: number;
  knownGroups?: string[];
  events?: RpcEvent[];
  intercept?: (call: DbCall) => DbResult | Promise<DbResult> | undefined;
} = {}) {
  const timeline: string[] = [];
  const tables: Record<string, Record<string, unknown>[]> = {
    groups: (options.knownGroups ?? []).map((id) => ({
      ...deriveGroupState(id, NO_FACTS),
    })),
    indexer_checkpoints: options.checkpoint === undefined ? [] : [{
      id: 'default',
      last_processed_ledger: options.checkpoint,
      start_ledger: FIRST_LEDGER,
      updated_at: '2026-01-01T00:00:00Z',
    }],
  };
  const stub = stubClient((call) => {
    timeline.push(`db:${call.table}:${call.operation}`);
    const intercepted = options.intercept?.(call);
    if (intercepted !== undefined) return intercepted;
    const table = tables[call.table] ??= [];
    const matches = (row: Record<string, unknown>) =>
      call.filters.every(({ column, value }) =>
        Array.isArray(value) ? value.includes(row[column]) : value === row[column]
      );
    if (call.operation === 'select') {
      const rows = table.filter(matches);
      return { data: call.single ? rows[0] ?? null : rows, error: null };
    }
    if (call.operation === 'update') {
      const rows = table.filter(matches);
      for (const row of rows) Object.assign(row, call.rows);
      return { error: null, count: rows.length };
    }
    const incoming = (Array.isArray(call.rows) ? call.rows : [call.rows]) as Record<
      string,
      unknown
    >[];
    for (const row of incoming) {
      const keys = String(call.options?.onConflict ?? '').split(',');
      const existing = call.operation === 'upsert'
        ? table.find((stored) => keys.every((key) => stored[key] === row[key]))
        : undefined;
      if (existing) {
        if (!call.options?.ignoreDuplicates) Object.assign(existing, row);
      } else {
        table.push({
          ...(call.table === 'groups' ? deriveGroupState(String(row.contract_id), NO_FACTS) : {}),
          ...row,
        });
      }
    }
    return { error: null };
  });
  const rpcCalls: (EventPageStart & { contractIds: string[]; limit?: number })[] = [];
  const events = options.events ?? allEvents;
  const rpc = {
    getLatestLedger() {
      timeline.push('rpc:latest');
      return Promise.resolve(LAST_LEDGER);
    },
    getEvents(params: EventPageStart & { contractIds: string[]; limit?: number }) {
      timeline.push('rpc:events');
      rpcCalls.push(params);
      assertEquals(params.kind, 'range');
      const page = events.filter((event) =>
        params.contractIds.includes(event.contractId) && params.kind === 'range' &&
        event.ledger >= params.startLedger && event.ledger < params.endLedger
      );
      return Promise.resolve({ events: page, latestLedger: LAST_LEDGER });
    },
  };
  return {
    db: new IndexerDb('https://unused.invalid', 'test-only', stub.client),
    rpc,
    calls: stub.calls,
    rpcCalls,
    timeline,
    tables,
  };
}

Deno.test('toIndexedRow derives chain identity and copies the topic array', () => {
  const event = allEvents[0]!;
  const row = entry.toIndexedRow(event);
  assertEquals(
    row.event_identity,
    `${event.contractId}:${event.ledger}:${event.txHash}:${event.eventIndex}`,
  );
  assertEquals(row.ledger, event.ledger);
  assertEquals(row.tx_hash, event.txHash);
  assertEquals(row.tx_index, event.txIndex);
  assertEquals(row.event_index, event.eventIndex);
  assertEquals(row.contract_id, event.contractId);
  assertEquals(row.topic, event.topic);
  assertEquals(row.value, event.value);
  assert(row.topic !== event.topic);
});

Deno.test('handler discovers same-range groups, ingests both passes and advances last', async () => {
  await withConfig(async () => {
    const h = harness();
    const response = await entry.handleRequest(invocation(), h);
    assertEquals(response.status, 200);
    assertEquals(response.headers.get('content-type'), 'application/json');
    const summary = await response.json();
    assertEquals(summary.status, 'ok');
    assertEquals(summary.eventsIndexed, allEvents.length);
    assertEquals(summary.eventsDecoded, allEvents.length);
    assertEquals(summary.eventsRejected, 0);
    assertEquals(summary.groupsDiscovered, factoryEvents.length);
    assertEquals(summary.checkpoint, LAST_LEDGER);
    assertEquals(summary.lag, 0);
    assert(typeof summary.correlationId === 'string');
    assertEquals(h.rpcCalls[0]?.contractIds, [FACTORY_ID, TOKEN]);
    assert(h.rpcCalls.slice(1).some((call) => call.contractIds.includes(GROUP_ID)));
    assert(h.rpcCalls.every((call) =>
      call.kind === 'range' && call.startLedger === FIRST_LEDGER &&
      call.endLedger === LAST_LEDGER + 1 && call.contractIds.length <= 5
    ));
    assertEquals(h.tables.indexed_events?.length, allEvents.length);
    assertEquals(h.tables.groups?.length, factoryEvents.length);
    const writes = h.calls.filter((call) => call.operation !== 'select');
    assertEquals(writes[0]?.table, 'groups');
    assertEquals(writes[1]?.table, 'indexed_events');
    assertEquals(writes.at(-1)?.table, 'indexer_checkpoints');
    assert(
      h.timeline.indexOf('db:groups:update') < h.timeline.indexOf('db:indexer_checkpoints:upsert'),
    );
  });
});

Deno.test('handler deduplicates replays, retains rejected raw events and excludes token decoding', async () => {
  await withConfig(async () => {
    const source = factoryEvents[0]!;
    const unknown: RpcEvent = {
      ...source,
      eventIndex: 90,
      topic: [symbolTopic('susu'), symbolTopic('unknown_event')],
    };
    const token: RpcEvent = { ...source, contractId: TOKEN };
    const h = harness({ events: [...allEvents, source, unknown, token] });
    const response = await entry.handleRequest(invocation(), h);
    const summary = await response.json();
    assertEquals(response.status, 200);
    assertEquals(summary.eventsIndexed, allEvents.length + 2);
    assertEquals(summary.eventsDecoded, allEvents.length);
    assertEquals(summary.eventsRejected, 1);
    assertEquals(h.tables.indexed_events?.length, allEvents.length + 2);
    assertEquals(h.tables.decoded_events?.length, allEvents.length);
    assertEquals(h.tables.indexer_checkpoints?.[0]?.last_processed_ledger, LAST_LEDGER);
  });
});

Deno.test('failed fact writes do not advance the checkpoint; recovery replays without duplicates', async () => {
  await withConfig(async () => {
    let fail = true;
    const h = harness({
      intercept(call) {
        if (fail && call.table === 'contributions' && call.operation === 'upsert') {
          return { error: { message: 'injected write failure' } };
        }
      },
    });
    await captureErrors(async () => {
      const failed = await entry.handleRequest(invocation(), h);
      assertEquals(failed.status, 500);
      const summary = await failed.json();
      assertEquals(summary.status, 'failed');
      assert(String(summary.reason).includes('injected write failure'));
    });
    assertEquals(h.calls.filter((call) => call.table === 'contributions').length, 4);
    assertEquals(
      h.calls.filter((call) => call.table === 'indexer_checkpoints' && call.operation === 'upsert'),
      [],
    );
    assertEquals(h.tables.indexer_runs?.length, 1);
    const rawCount = h.tables.indexed_events?.length;
    fail = false;
    const recovered = await entry.handleRequest(invocation(), h);
    assertEquals(recovered.status, 200);
    const summary = await recovered.json();
    assertEquals(summary.ledgerFrom, FIRST_LEDGER);
    assertEquals(summary.groupsDiscovered, 0);
    assertEquals(h.tables.indexed_events?.length, rawCount);
    assertEquals(h.tables.indexer_checkpoints?.[0]?.last_processed_ledger, LAST_LEDGER);
    assertEquals(
      h.calls.filter((call) => call.table === 'indexer_checkpoints' && call.operation === 'upsert')
        .length,
      1,
    );
  });
});

Deno.test('a rejected failure-record insert preserves structured 500 and the original error', async () => {
  await withConfig(async () => {
    const privateDetail = 'insert transport credential must not appear';
    const h = harness({
      intercept(call) {
        if (call.table === 'indexed_events') {
          return { error: { message: 'original indexing failure' } };
        }
        if (call.table === 'indexer_runs') return Promise.reject(new Error(privateDetail));
      },
    });
    const lines = await captureErrors(async () => {
      const response = await entry.handleRequest(invocation(), h);
      assertEquals(response.status, 500);
      const summary = await response.json();
      assertEquals(summary.status, 'failed');
      assert(String(summary.reason).includes('original indexing failure'));
      assertEquals(response.headers.get('content-type'), 'application/json');
    });
    assertEquals(lines.length, 2);
    assert(!lines.join('').includes(privateDetail));
    assertEquals(
      h.calls.filter((call) => call.table === 'indexer_checkpoints' && call.operation === 'upsert'),
      [],
    );
  });
});

Deno.test('a failed reconciliation cannot advance the checkpoint', async () => {
  await withConfig(async () => {
    const h = harness({
      intercept(call) {
        if (call.table === 'groups' && call.operation === 'update') {
          return { error: null, count: 0 };
        }
      },
    });
    await captureErrors(async () => {
      const response = await entry.handleRequest(invocation(), h);
      assertEquals(response.status, 500);
      assert(String((await response.json()).reason).includes('no groups row'));
    });
    assertEquals(
      h.calls.filter((call) => call.table === 'indexer_checkpoints' && call.operation === 'upsert'),
      [],
    );
  });
});

Deno.test('caught-up runs skip all event reads and writes', async () => {
  await withConfig(async () => {
    const h = harness({ checkpoint: LAST_LEDGER });
    const response = await entry.handleRequest(invocation(), h);
    const summary = await response.json();
    assertEquals(response.status, 200);
    assertEquals(summary.status, 'skipped');
    assertEquals(summary.checkpoint, LAST_LEDGER);
    assertEquals(summary.lag, 0);
    assertEquals(h.rpcCalls, []);
    assertEquals(h.calls.map((call) => call.operation), ['select']);
  });
});

Deno.test('authorization rejects before accessing either data boundary', async () => {
  await withConfig(async () => {
    const h = harness();
    const response = await entry.handleRequest(invocation('wrong-secret'), h);
    assertEquals(response.status, 401);
    assertEquals((await response.json()).reason, 'unauthorized');
    assertEquals(h.timeline, []);
  });
});

Deno.test('invalid configuration returns 500 with variable names and no secret values', async () => {
  await withConfig(async () => {
    const h = harness();
    await captureErrors(async () => {
      const response = await entry.handleRequest(invocation(), h);
      assertEquals(response.status, 500);
      const summary = await response.json();
      assertEquals(summary.status, 'failed');
      assert(String(summary.reason).includes('FACTORY_CONTRACT_ID'));
      assert(!JSON.stringify(summary).includes(SECRET));
    });
    assertEquals(h.timeline, []);
  }, { FACTORY_CONTRACT_ID: undefined });
});
