import { assert, assertEquals, assertRejects } from '@std/assert';
import { IndexerDb } from '../supabase/functions/_shared/db.ts';
import { discoverGroups } from '../supabase/functions/_shared/discovery.ts';
import { planIngest } from '../supabase/functions/_shared/ingest.ts';
import { deriveGroupState, NO_FACTS } from '../supabase/functions/_shared/state.ts';
import { captureErrors, type DbResult, stubClient } from './db_stub.ts';
import { allEvents, decodeOk, FACTORY_ID, GROUP_ID } from './fixture.ts';

function database(respond?: Parameters<typeof stubClient>[0]) {
  const stub = stubClient(respond);
  return { ...stub, db: new IndexerDb('https://unused.invalid', 'test-only', stub.client) };
}

const failure = {
  correlationId: 'test-correlation',
  ledgerFrom: 10,
  ledgerTo: 20,
  reason: 'original failure',
};

Deno.test('recordRunFailure writes bounded metadata and is silent on success', async () => {
  const { db, calls } = database();
  const lines = await captureErrors(() =>
    db.recordRunFailure({ ...failure, reason: 'x'.repeat(700) })
  );
  assertEquals(lines, []);
  assertEquals(calls[0], {
    table: 'indexer_runs',
    operation: 'insert',
    filters: [],
    rows: {
      correlation_id: failure.correlationId,
      ledger_from: 10,
      ledger_to: 20,
      status: 'failed',
      reason: 'x'.repeat(500),
    },
  });
});

for (const mode of ['returned error', 'rejected insert', 'synchronous client failure']) {
  Deno.test(`recordRunFailure never rejects on ${mode} or exposes transport details`, async () => {
    const sensitive = 'transport error with service-role credential';
    const { db, client } = database(() => {
      if (mode === 'returned error') return { error: { message: sensitive } };
      return Promise.reject(new Error(sensitive));
    });
    if (mode === 'synchronous client failure') {
      client.from = () => {
        throw new Error(sensitive);
      };
    }
    const lines = await captureErrors(() => db.recordRunFailure(failure));
    assertEquals(lines.length, 1);
    assertEquals(JSON.parse(lines[0]!), {
      level: 'error',
      message: 'Failed to record indexer run failure',
      correlationId: failure.correlationId,
    });
    assert(!lines.join('').includes(sensitive));
  });
}

Deno.test('getCheckpoint handles a first run and converts an existing checkpoint', async () => {
  let response: DbResult = { data: null, error: null };
  const { db, calls } = database(() => response);
  assertEquals(await db.getCheckpoint(), undefined);
  response = {
    data: { last_processed_ledger: '40', start_ledger: '10', updated_at: '2026-01-01' },
    error: null,
  };
  assertEquals(await db.getCheckpoint(), {
    lastProcessedLedger: 40,
    startLedger: 10,
    updatedAt: '2026-01-01',
  });
  assertEquals(calls[0]?.filters, [{ column: 'id', value: 'default' }]);
  assertEquals(calls[0]?.single, true);
});

Deno.test('group watch-list handles empty and populated results', async () => {
  let data: unknown = null;
  const { db } = database(() => ({ data, error: null }));
  assertEquals(await db.listGroupContractIds(), []);
  data = [{ contract_id: GROUP_ID }];
  assertEquals(await db.listGroupContractIds(), [GROUP_ID]);
});

Deno.test('empty writes and empty group reads never send a database request', async () => {
  const { db, calls } = database();
  await db.upsertEvents([]);
  await db.upsertGroups([]);
  await db.persistPlan(planIngest([]));
  await db.upsertGroupState([]);
  assertEquals((await db.readGroupFacts([])).size, 0);
  assertEquals((await db.readGroupState([])).size, 0);
  assertEquals(calls, []);
});

Deno.test('discovery and fact writes preserve the replay conflict contracts', async () => {
  const { db, calls } = database();
  const events = allEvents.map(decodeOk);
  const groups = discoverGroups(events, [FACTORY_ID]);
  await db.upsertGroups(groups);
  await db.upsertEvents([{
    event_identity: 'chain-derived',
    ledger: 10,
    tx_hash: 'a'.repeat(64),
    tx_index: 0,
    event_index: 1,
    contract_id: GROUP_ID,
    topic: [],
    value: '',
  }]);
  await db.persistPlan(planIngest(events));
  assertEquals(calls.map((call) => call.table), [
    'groups',
    'indexed_events',
    'decoded_events',
    'group_members',
    'contributions',
    'payouts',
    'protocol_fees',
  ]);
  assertEquals(calls[0]?.rows, groups);
  assertEquals(calls.map((call) => call.options), [
    { onConflict: 'contract_id', ignoreDuplicates: true },
    { onConflict: 'event_identity', ignoreDuplicates: true },
    { onConflict: 'event_identity', ignoreDuplicates: true },
    { onConflict: 'contract_id,member', ignoreDuplicates: true },
    { onConflict: 'event_identity', ignoreDuplicates: true },
    { onConflict: 'event_identity', ignoreDuplicates: true },
    { onConflict: 'event_identity', ignoreDuplicates: true },
  ]);
});

Deno.test('readGroupFacts preserves exact money and aggregates lifecycle and last ledger', async () => {
  const amount = '170141183460469231731687303715884105727';
  const rows: Record<string, unknown[]> = {
    group_members: [{ contract_id: GROUP_ID, position: '1' }],
    contributions: [{ contract_id: GROUP_ID, round: '2', amount }],
    payouts: [{ contract_id: GROUP_ID, round: '2', recipient_amount: '123' }],
    protocol_fees: [{ contract_id: GROUP_ID, round: '2', fee: '3' }],
    decoded_events: [
      { contract_id: GROUP_ID, name: 'start', ledger: '40' },
      { contract_id: GROUP_ID, name: 'completed', ledger: '45' },
      { contract_id: GROUP_ID, name: 'join', ledger: '42' },
    ],
  };
  const { db, calls } = database((call) => ({ data: rows[call.table], error: null }));
  const facts = await db.readGroupFacts([GROUP_ID]);
  assertEquals(facts.get(GROUP_ID), {
    members: [{ position: 1 }],
    contributions: [{ round: 2, amount }],
    payouts: [{ round: 2, recipient_amount: '123' }],
    fees: [{ round: 2, fee: '3' }],
    started: true,
    completed: true,
    lastEventLedger: 45,
  });
  assertEquals(
    calls.filter((call) => /total|amount|fee/.test(call.columns ?? '')).map((c) => c.columns),
    [
      'contract_id, round, amount::text',
      'contract_id, round, recipient_amount::text',
      'contract_id, round, fee::text',
    ],
  );
  assert(calls.every((call) => call.filters[0]?.column === 'contract_id'));
  assertEquals((await database().db.readGroupFacts([GROUP_ID])).size, 0);
});

Deno.test('readGroupState coerces counters and preserves exact totals', async () => {
  const state = deriveGroupState(GROUP_ID, NO_FACTS);
  const { contract_id, ...storedState } = state;
  assertEquals(contract_id, GROUP_ID);
  const { db, calls } = database(() => ({
    data: [{
      ...state,
      member_count: '3',
      current_round: '2',
      contributed_total: '9007199254740993',
    }],
    error: null,
  }));
  assertEquals((await db.readGroupState([GROUP_ID])).get(GROUP_ID), {
    ...storedState,
    member_count: 3,
    current_round: 2,
    contributed_total: '9007199254740993',
  });
  assert(calls[0]?.columns?.includes('contributed_total::text'));
  assert(calls[0]?.columns?.includes('paid_out_total::text'));
  assert(calls[0]?.columns?.includes('fee_total::text'));
  assertEquals((await database().db.readGroupState([GROUP_ID])).size, 0);
});

Deno.test('reconciliation updates existing rows with exact counts instead of inserting them', async () => {
  const { db, calls } = database(() => ({ error: null, count: 1 }));
  const state = deriveGroupState(GROUP_ID, NO_FACTS);
  await db.upsertGroupState([state]);
  const call = calls[0]!;
  assertEquals(call.operation, 'update');
  assertEquals(call.filters, [{ column: 'contract_id', value: GROUP_ID }]);
  assertEquals(call.options, { count: 'exact' });
  const rows = call.rows as Record<string, unknown>;
  assertEquals(rows.contributed_total, '0');
  assertEquals('contract_id' in rows, false);
  assert(Number.isFinite(Date.parse(String(rows.updated_at))));
  await assertRejects(
    () => database(() => ({ error: null, count: 0 })).db.upsertGroupState([state]),
    Error,
    'no groups row',
  );
});

Deno.test('advanceCheckpoint targets the stable row and writes integer ledgers', async () => {
  const { db, calls } = database();
  await db.advanceCheckpoint({ lastProcessedLedger: 40, startLedger: 10 });
  assertEquals(calls[0]?.table, 'indexer_checkpoints');
  assertEquals(calls[0]?.options, { onConflict: 'id' });
  const rows = calls[0]?.rows as Record<string, unknown>;
  assertEquals(rows.id, 'default');
  assertEquals(rows.last_processed_ledger, 40);
  assertEquals(rows.start_ledger, 10);
  assert(Number.isFinite(Date.parse(String(rows.updated_at))));
});

Deno.test('database failures propagate from every required read and write', async () => {
  const { db } = database(() => ({ error: { message: 'database unavailable' } }));
  const events = allEvents.map(decodeOk);
  const groups = discoverGroups(events, [FACTORY_ID]);
  const operations: (() => Promise<unknown>)[] = [
    () => db.getCheckpoint(),
    () => db.listGroupContractIds(),
    () => db.upsertGroups(groups),
    () =>
      db.upsertEvents([{
        event_identity: 'identity',
        ledger: 1,
        tx_hash: 'a'.repeat(64),
        tx_index: 0,
        event_index: 0,
        contract_id: GROUP_ID,
        topic: [],
        value: '',
      }]),
    () => db.persistPlan(planIngest(events)),
    () => db.readGroupFacts([GROUP_ID]),
    () => db.readGroupState([GROUP_ID]),
    () => db.upsertGroupState([deriveGroupState(GROUP_ID, NO_FACTS)]),
    () => db.advanceCheckpoint({ lastProcessedLedger: 40, startLedger: 10 }),
  ];
  for (const operation of operations) {
    await assertRejects(operation, Error, 'database unavailable');
  }
});
