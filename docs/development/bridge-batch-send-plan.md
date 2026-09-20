# Bridge batch-send plan

## Purpose

Add safe bulk history import for mautrix bridges. This is for local bridge rooms,
not federated history insertion.

The wire API is named `com.beeper.backfill` because Beeper originally defined the
extension and mautrix-go calls that exact path. Using the name does not require a
Beeper account, server, client, or service.

## Required API

```text
POST /_matrix/client/unstable/com.beeper.backfill/rooms/{room_id}/batch_send
```

Discovery uses:

```json
{"unstable_features":{"com.beeper.batch_sending":true}}
```

The request contains:

- `events`, in oldest-to-newest order
- `forward`: append to the live end
- `forward_if_no_messages`: append when the room has no messages, otherwise prepend
- `send_notification`: notify only for forward batches
- `mark_read_by`: move one user's read receipt to the final event

The response is `{"event_ids":[...]}` in request order.

Current mautrix-go supplies event IDs and assumes the homeserver preserves them
exactly. It discards the returned IDs and stores its precomputed IDs, so changing
an ID silently breaks replies, reactions, and bridge database mappings.

## Simple explanation

A Matrix event is not one database row. Saving one can update the timeline,
state snapshot, room end, replies, reactions, threads, receipts, unread counts,
search, notifications, and delivery queues.

Tuwunel currently saves those pieces one event at a time. Calling the existing
append function repeatedly can therefore expose half a batch if a later event
fails. Prepending is harder because old events must appear before live events
without rewinding the room's current state or moving its live end.

The solution is to prepare and validate the whole batch in memory, then write all
core database rows with one existing RocksDB `WriteBatch`. Only after that atomic
commit may Tuwunel publish notifications or other external effects.

## Blockers

1. Existing append and backfill helpers commit one event at a time.
2. State snapshots and room-frontier helpers perform immediate writes.
3. Caller-supplied deterministic event IDs need a local-only construction path.
4. Negative backfill counts are allocated in the opposite order from the
   oldest-to-newest request.
5. Relation indexes currently ignore backfilled events.
6. HTTP retries have no transaction ID, so exact event-ID replay must be
   idempotent.
7. `Txn::execute()` currently panics instead of returning a recoverable error.
8. Notifications and appservice delivery happen outside the core timeline write
   and need idempotent post-commit handling.
9. Double-puppet senders are outside a bridge's ghost namespace and need an
   explicit allowlist; trusting every local user would be unsafe.

## Safe design

### Gates

Keep support disabled by default. When disabled, return `404` and do not advertise
the discovery flag.

Enable only when federation is disabled. Allow only configured appservice IDs.
An event sender must either match the calling appservice namespace or be an
explicitly allowed local double-puppet user. Require normal room membership and
authorization.

Initially accept only the event forms actually produced by the pinned WhatsApp
and Instagram bridges. Reject state events, malformed or duplicate IDs, room
mismatches, oversized events, and unsupported flag combinations before writing.
An empty event list is a valid no-op because mautrix-go can send one.

### Atomic planner

Add `src/service/rooms/timeline/batch.rs`.

Under the room-state lock and then the timeline-insertion lock:

1. Resolve forward versus backward behavior and select the state snapshot.
2. Validate every event and supplied event ID.
3. Treat an all-existing identical batch as a successful retry.
4. Reject conflicting IDs or a partially existing batch.
5. Authenticate, hash, sign, and construct every event in memory.
6. Preserve each supplied event ID exactly.
7. Allocate timeline counts only after validation; hold all count permits through
   commit so `/sync` cannot pass unpublished events.
8. Stage timeline rows, event-ID mappings, timestamp indexes, state-at-event
   mappings, references, and supported relation indexes in one database `Txn`.
9. For a forward batch, stage the final room frontier and explicit read receipt.
10. Execute once, then retire counts and run idempotent post-commit effects.

Add a recoverable `Txn::try_execute() -> Result<()>`. Keep the existing
`execute()` wrapper for unrelated callers.

A failed commit may consume unused global count numbers. Gaps are harmless; no
batch event may become visible.

### Forward order

For `[oldest, ..., newest]`, assign increasing normal counts and build:

```text
existing leaves -> oldest -> ... -> newest
```

Publish only `newest` as the final live frontier.

### Backward order

For `[oldest, middle, newest]` and newly allocated numbers `101..103`, assign:

```text
oldest = -103
middle = -102
newest = -101
```

This keeps database pagination chronological. Use the state and predecessors of
the earliest existing message as the historical anchor. Build an internal chain,
but do not rewrite that existing event, current room state, memberships, or live
forward frontier.

This is a local timeline projection. It is intentionally not a federation-valid
DAG splice and uses no MSC2716 insertion markers.

### Side effects

Backward history must not generate pushes, federation traffic, importer echo,
admin commands, invite acceptance, membership changes, current-state changes, or
frontier changes.

Forward batches may need notifications and delivery to other interested local
appservices. Queue those only after commit and key work by event ID so a retry or
crash cannot duplicate it. `mark_read_by` belongs in the atomic database batch.

Encrypted events remain opaque. Tuwunel must not decrypt or rewrite ciphertext.
Cleartext reactions and other visible relations need transaction-aware relation
indexing, including negative event counts.

## Delivery order

1. Add recoverable transactions and transaction-aware staging helpers.
2. Implement fresh/state-only portal import with deterministic IDs and retries.
3. Implement live-room prepend with negative counts and unchanged current state.
4. Add backfilled relation/thread indexes required by real bridge fixtures.
5. Add forward notification and local appservice-delivery parity.
6. Run end-to-end WhatsApp and Instagram fixtures.
7. Advertise `com.beeper.batch_sending` only after every emitted request form is
   supported. Keep queue backfill disabled until then.

## Required checks

- Invalid event N leaves zero visible events from the batch.
- IDs and response order exactly match the request.
- Exact retry creates no duplicate rows or side effects.
- Conflicting and partial retries fail atomically.
- Forward history has correct order and final frontier.
- Prepended history has correct order after restart.
- Prepend does not change current state or live frontier.
- A concurrent live send cannot interleave with planning or commit.
- Same-batch replies and reactions resolve correctly.
- Unauthorized appservices, ghosts, double puppets, and receipt users fail.
- Backward history produces no push, federation, or importer echo.

## Explicit non-goals

- MSC2716 insertion/chunk/marker events
- Federation-valid historical DAGs
- Arbitrary middle-of-room insertion
- Historical state-event reconstruction
- Decryption or custom cryptography
- Migrating RocksDB to `TransactionDB`
- Implementing the endpoint as a loop over existing per-event append methods
