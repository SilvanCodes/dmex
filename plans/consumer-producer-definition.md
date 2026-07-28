# Consumer-side producer definition

> Status: **implemented, plus one follow-up.** This is the design record for the
> refactor; the code now matches it, except that `messages:` no longer exists —
> a follow-up removed it and derives the list from the structs
> `handle_message/2` matches on (see Critique #4). Read every `messages:` below
> as "the types the handler's clauses match on".
>
> Two further things landed differently from the text below, both noted in
> place: `subscribe/2` also accepts `producer:` as a call-site override (the
> same escape hatch `repo:` has), and `@subscription_opts` is `[:consumer,
> :producer]` rather than `[:handler, :consumer]`, since the handler is the
> positional argument and a stray `handler:` should raise rather than be
> ignored.

## Context

Today a subscription is assembled entirely at the call site:

```elixir
DMEx.Consumer.subscribe("https://orders.internal/stream",
  message_modules: [MyApp.Messages.OrderPlaced],
  handler: MyApp.OrderHandler
)
```

and its durable identity — the key of `dmex_cursors` and half the key of
`dmex_dead_messages` — is `inspect(MyApp.OrderHandler)`. Two things follow that
the draft documents at length and neither of which is good:

- A handler's identity is its **module name**, so an ordinary rename resets its
  cursor and replays the producer's whole log. `DMEx.Consumer.Cursors.rename!/3`
  exists solely to paper over this, and `DMEx.Consumer.Handler`'s moduledoc has
  to warn about it.
- The producer is named by **URL literal**, threaded through the child spec and
  repeated wherever a subscription is declared.

This refactor makes a subscription's identity an explicit, declared, stable pair
— `{producer name, handler id}` — living on the handler module itself, and binds
producer names to URLs in one map on the consumer. Message types move onto the
handler too, so the handler module becomes the complete declaration of what this
consumer consumes and from whom.

Greenfield: no backwards compatibility, no data migration path, no deprecation
shims. Columns are renamed outright and `message_modules:`/`url:` are removed
rather than deprecated.

### Decisions taken

- The producer map lives on the **`DMEx.Consumer` child spec**, not in
  application env — `DMEx.Config`'s "DMEx reads no global application config"
  stance stays true, two named consumers keep working, and tests can inject a
  port-0 Bandit URL at runtime.
- Identity is `{producer, id}` where `id` is an **explicit human-chosen string**.
  The message list is declared on the handler but does **not** participate in
  identity — see [Critique](#critique) for what that costs.
- The identifier is a **readable composite string**, not a digest:
  `"orders/order_projection"`.

---

## Target API

```elixir
defmodule MyApp.OrderProjection do
  use DMEx.Consumer.Handler,
    producer: :orders,
    id: :order_projection,
    messages: [MyApp.Messages.OrderPlaced, MyApp.Messages.OrderCancelled]

  @impl true
  def handle_message(%MyApp.Messages.OrderPlaced{} = message, _metadata), do: :ok
  def handle_message(%MyApp.Messages.OrderCancelled{} = message, _metadata), do: :ok
end
```

```elixir
{DMEx.Consumer,
 repo: MyApp.Repo,
 producer: %{
   orders:  "https://orders.internal/stream",
   billing: "https://billing.internal/stream"
 },
 handlers: [
   MyApp.OrderProjection,
   {MyApp.OrderEmail, on_failure: :dead_message, max_batch_size: 100}
 ]}
```

```elixir
DMEx.Consumer.subscribe(MyApp.OrderProjection, on_failure: :skip)
DMEx.Consumer.unsubscribe(MyApp.OrderProjection)
```

`subscribe/2`'s first argument becomes the handler module. `:url` and
`:message_modules` cease to exist as options.

### The identifier

`"#{producer}/#{id}"` — e.g. `"orders/order_projection"`.

Both halves are constrained to `~r/\A[a-z][a-z0-9_]*\z/`, mirroring
`DMEx.Producer`'s `@app_key_format` (`lib/dmex/producer.ex:82`). That charset is
**load-bearing**: it is what makes `/` an unambiguous separator with no escaping,
so two distinct pairs can never render to one string. Accept the consequence —
an id cannot be a UUID, contain a dot, or contain a dash.

This string is a **persistent format**. Changing the separator or the charset
later resets every cursor in every deployment. Pin it with a test asserting the
exact literal for a known declaration.

---

## Changes

### 1. `lib/dmex/consumer/handler.ex` — rewrite

`__using__/1` currently raises on any option
(`lib/dmex/consumer/handler.ex:70-85`). It now takes exactly three, all required:

- `:producer` — atom or binary, must match the identifier format.
- `:id` — atom or binary, must match the identifier format.
- `:messages` — non-empty list of atoms.

Any other key raises, with the `DMEx.Producer.validate_keys!/1` error shape
(`lib/dmex/producer.ex:199`) as the model.

Injects, replacing `__dmex_handler__/0`:

```elixir
@behaviour DMEx.Consumer.Handler
@dmex DMEx.Consumer.Handler.__declaration__(__MODULE__, opts)

@doc false
def __dmex__, do: @dmex
```

where `__declaration__/2` returns

```elixir
%{
  handler: MyApp.OrderProjection,
  producer: "orders",
  id: "orders/order_projection",
  messages: [MyApp.Messages.OrderPlaced, MyApp.Messages.OrderCancelled]
}
```

Use `quote bind_quoted: [opts: opts]` as `DMEx.Producer.__using__/1` does, so the
declaration is built once at compile time rather than per call.

**Validate shape only at compile time — do not touch the message modules.**
Calling `module.type()` or `Code.ensure_compiled?/1` from inside the macro
creates a compile-time dependency on every declared message module, so touching
one struct recompiles every handler that lists it. Existence and
`use DMEx.Message`-ness are checked at runtime in `Pipeline.build/1`, which
already calls `type/0` via `DMEx.Message.registry/1`.

Public functions:

- `handler?/1` — keep, retarget to `__dmex__/0`.
- `subscription_id/1` — `handler.__dmex__().id`. The one place anything converts
  a module to its durable identity.

### 2. `lib/dmex/message.ex` — add `message?/1`

Mirror of `Handler.handler?/1`, so `Pipeline.build/1` can reject a module in
`:messages` that is not a `DMEx.Message` with a decent error rather than an
`UndefinedFunctionError` on `type/0`.

### 3. `lib/dmex/consumer.ex`

- `@subscription_opts` (`consumer.ex:78`) → `[:consumer, :producer]`. **Landed
  differently:** `:handler` is left out deliberately — it is the positional
  argument now, so a stray `handler:` in the options is a mistake worth raising
  on rather than silently ignoring. `:producer` is in for the override below.
- `:subscriptions` option → `:handlers`, a list of `module` or
  `{module, keyword}`.
- New `:producer` option: a map of name → URL. Validate at `init/1` — keys match
  the identifier format, values are non-empty binaries with an `http`/`https`
  scheme and a host. Raise naming the offending entry.

  **Landed differently:** `subscribe/2` accepts `producer:` too, resolving it
  exactly as it resolves `repo:` — call-site map first, then the running
  consumer's registry metadata, then a raise naming both fixes. Without this a
  `subscribe/2` had no way at all to name a URL, since `:url` is gone; with it
  the two resolutions are symmetric. A consumer still has to be running either
  way, because it owns the registry each subscription registers in.
- Seed the map into registry metadata alongside `:storage`, reusing the existing
  trick at `consumer.ex:104` so `subscribe/2` resolves with no GenServer call:

  ```elixir
  {Registry, keys: :unique, name: registry(name),
   meta: [storage: storage_config!(opts), producer: producer_map!(opts)]}
  ```

- `subscribe/2` becomes `subscribe(handler, opts \\ [])`:
  1. `Handler.handler?/1` guard, with the existing error message.
  2. Read `handler.__dmex__()`.
  3. Resolve `producer` against the map. **Unknown name raises**, naming the
     declared name and listing the known ones.
  4. Merge `url`, `subscription` (the id), `messages`, storage, and
     `Config.consumer!(opts)` into the child opts.
- `unsubscribe/2` looks up `{:subscription, id}`.
- **Duplicate-id detection belongs in `init/1`, not in `start_configured/2`.**
  The latter runs inside a `Task` with `restart: :temporary`
  (`consumer.ex:106,114`), so a raise there kills the Task silently and the
  supervisor never notices — not the loud failure required. Computing every
  configured handler's id is a pure function over the `:handlers` list, so do it
  in `init/1` before any child starts and raise with both module names. The
  registry key change gives the same guarantee for dynamic `subscribe/2`, which
  keeps returning `{:error, {:already_subscribed, id}}`.

### 4. `lib/dmex/consumer/stream.ex`

- `child_spec` id (`stream.ex:83`) → `{__MODULE__, subscription_id}`.
- `register/2` (`stream.ex:570-574`) → key `{:subscription, id}`;
  `{:stop, {:already_subscribed, id}}`.
- State gains `:subscription` (the id string). **Keep `:handler`** (the module) —
  it is what invokes the callback and what a human reads in telemetry and logs.
- `durable_cursor/1` (`stream.ex:227`) passes the id.

### 5. `lib/dmex/consumer/pipeline.ex` and `failure.ex`

- `Pipeline.t()` gains `:subscription`.
- `build/1` (`pipeline.ex:209`) reads `handler.__dmex__().messages` instead of
  `Keyword.fetch!(opts, :message_modules)`, and validates each via
  `Message.message?/1` before `DMEx.Message.registry/1`.
- `advance!/2` (`pipeline.ex:198`) and the dead-message write
  (`failure.ex:99`) pass `state.subscription`.
- Telemetry: **add** `:subscription` to the metadata maps, **keep** `:handler`.
  Document both in `lib/dmex/telemetry.ex`.

### 6. `lib/dmex/storage.ex` and `lib/dmex/storage/postgres.ex`

Six callbacks change `handler :: module()` → `subscription :: String.t()`:
`last_seen/2`, `cursor/2`, `delete_cursor/2`, `advance/4`, `dead_messages/2`,
`delete_dead_message/3`. `dead_message/2`'s `attrs` map key `:handler` →
`:subscription`. Guards `is_atom` → `is_binary`.

In the Postgres adapter this **deletes** all seven `inspect(handler)` calls
(`postgres.ex:105,122,139,165,181,209,221`) — the id arrives as a string and goes
straight into the query. Update the moduledoc note in `DMEx.Storage` that
currently says "Keyed by handler module because the handler *is* the
subscription's identity" (`storage.ex:129`).

### 7. `lib/dmex/migration/postgres.ex`

- `dmex_cursors.handler` → `dmex_cursors.subscription` (`:text`, primary key).
- `dmex_dead_messages.handler` → `dmex_dead_messages.subscription` (`:text`,
  first half of the composite PK with `id`).
- Rewrite the two long column comments (`postgres.ex:72-78` and `:105-109`),
  which currently explain the module-name keying and the `inspect/1` storage.
- **No extra informational column.** The id is readable and greppable; adding a
  `handler_module` column would be a second thing to keep in step for no lookup.
- Keep `producer_url` on both tables — still provenance, still not identity.

### 8. `lib/dmex/consumer/cursors.ex`

- `inspect/2`, `move!/4`, `reset!/2` take the handler module and resolve via
  `Handler.subscription_id/1`.
- **Keep `rename!/3`, retargeted to id strings.** The migration surface moved
  rather than vanished: a module rename is now free, but changing a handler's
  `id:` or a producer's map key resets that cursor exactly as a rename used to.
  Rewrite the moduledoc's rename section accordingly — it is the honest
  replacement, not a leftover.

### 9. `lib/dmex/consumer/dead_messages.ex`

- `list/2`, `replay/2`, `discard/3` take the handler module, resolve the id.
- **`replay/2` drops `message_modules:`** (`dead_messages.ex:86`) — it reads the
  list off the handler. Passing it should raise rather than be ignored.

### 10. Documentation

`ARCHITECTURE.md` is the spec of record and must move with the code:

- `## Consuming API` (L400-550) — the `subscribe/2` sketch and the whole
  "**`last_seen` is keyed by handler module**" bullet with its two consequences.
- `### Consumer side` (L973-1049) — dead-messages table shape.
- `## Settled by the v1 implementation`, consumer-schema bullet (~L2097) — why
  the tables key on what they key on.

Plus: `DMEx.Consumer.Handler` and `DMEx.Consumer` moduledocs (both currently
document the module-as-identity model at length), `DMEx.Consumer.Cursors`,
`DMEx.Storage`, `DMEx.Migration.Postgres` comments, `lib/dmex/telemetry.ex`, and
the `README.md` quickstart.

### 11. Tests

- **`test/support/handlers.ex`** — the largest test-side change. All eight
  handlers need `producer:`/`id:`/`messages:`, and because the message list is
  now baked into the module, a handler reused across tests with different lists
  must become several modules. Budget for the file roughly doubling.
  (The follow-up that removed `messages:` turned this into "each handler matches
  its structs in the clause head"; the module-per-combination point stands.)
- **`test/support/server.ex` / `case.ex` / `integration_case.ex`** — Bandit binds
  port 0, so the producer map is built at runtime; add a helper that turns a
  running test server into `%{test: url}`.
- **`test/support/storage_stub.ex`** — keys by binary now.
- **`handler_test.exs`** — rewrite; its six tests pin the "takes no options"
  rule that is being removed.
- **`cursors_test.exs`** (16 tests) — `rename!/3` cases retarget to id strings.
- **`dead_messages_test.exs`** (16 tests) — drop `message_modules:`.
- **`consumer_test.exs`** — subscribe arity, unknown-option message, duplicate
  rejection.
- New coverage: id derivation (exact literal, frozen), duplicate id raising at
  `init/1` with both module names, unknown producer name raising with the known
  names listed, malformed `producer:`/`id:`/`messages:`, unknown `use` option,
  malformed producer-map entry.

---

## Critique

### What is actually bought, and what is not

**(a) "Changing the producer host does not require a migration" was already
true.** The cursor is keyed by handler module today; `producer_url` is
informational and identity for nothing. Moving a producer to a new host has never
needed a migration. What this change genuinely buys on that axis is naming — one
map entry instead of a URL literal repeated at every declaration site — which is
configuration hygiene, not migration avoidance. Worth having, worth not
overclaiming.

**(b) "Renaming a handler module does not require a migration" is real, and is
the substance of the change.** It is also the one the old design got most wrong,
because a rename is something a refactoring tool does without asking.

**But the migration surface moved rather than disappeared.** Changing `id:` or a
producer map key resets a cursor exactly as a rename used to. This is a strict
improvement — both are deliberate acts, neither is something an IDE does — but
"no migration ever" is not on offer, and `Cursors.rename!/3` has to survive to
serve them.

### The strongest argument against the choice we made

Hashing the message list — the original proposal — makes *adding a message type
to a handler* mean "mint a new identity, replay from the beginning, so this
handler sees that type's history." That is frequently what you actually want, and
it is automatic.

Not hashing it makes the same change mean "handle it from now on"; getting the
history requires a deliberate `Cursors.move!/4`. Cheap by default instead of safe
by default.

We chose cheap because the failure mode of the safe version is worse: a routine
additive change silently replays the *entire* log, re-delivering every message
for the types already processed (duplicate suppression is by cursor, and the new
identity has no cursor), and silently orphans that handler's existing dead
messages under an id nothing looks up any more. But the trade is real, and the
docs must point at `Cursors.move!/4` from wherever "adding a message type" is
described. This interacts with the deferred server-side selection
(ROADMAP.md § Message-Type Selection): once `messages:` becomes a
`/stream/<filter blob>` on the wire, "added a type, didn't move the cursor"
means the producer never sends that type's history at all.

### Where this design falls short

1. **`id:` is unenforceable.** Nothing structurally prevents copy-pasting a
   handler and forgetting to change `id:` (caught — duplicate check) or changing
   `id:` during a refactor because it looked like a name (not caught — it just
   resets). The only real defense is that it sits in the `use` line next to
   `producer:`, not in a config file three directories away.

2. **A handler now has two names.** `MyApp.Orders.ProjectionHandler` with
   `id: :order_projection` is the normal state after one refactor — which is the
   *point*, but it means operators reading `dmex_cursors` and developers reading
   code are looking at different strings. A readable composite id mitigates this;
   nothing eliminates it.

3. **The producer name is unverifiable against reality.** The map is the only
   binding between a logical name and a host. A `producer:` pointing at the wrong
   entry, or a map entry pointing at the wrong host, yields a subscription that
   looks perfectly healthy — connected, cursor advancing, every row skipped as an
   unknown type — while reading the wrong log. Skips are already the normal case,
   so they are not a signal. Catching this needs a producer that advertises its
   own identity on `/stream`, which is a producer-side change not in scope here.

4. ~~**The declaration is not checked against the implementation.**~~
   **Resolved by the follow-up.** This plan shipped a `messages:` list that
   could disagree with the clauses: a handler declaring
   `[OrderPlaced, OrderCancelled]` whose only clause matched `%OrderPlaced{}`
   got a `FunctionClauseError` on the first cancellation, at runtime, straight
   into the retry-then-`on_failure` path.

   The follow-up went further than the check this section imagined. Rather than
   comparing a declared list against the matched structs, `messages:` was
   **removed** and the list is now *derived* in `@before_compile` from the
   structs `handle_message/2` matches on — so the two cannot disagree, because
   there is only one of them. Each matched struct is verified to be a
   `DMEx.Message` at compile time, and a clause that does not match a struct is
   rejected.

   Two costs it introduced, both accepted: the verification needs
   `Code.ensure_compiled/1` (the parallel compiler resolves a struct pattern
   without loading the module), which turns an export dependency into a
   compile-time one — bounded, since handlers are leaves; and derivation covers
   types, not values, so a clause narrowed to `%OrderPlaced{status: "paid"}`
   still declares `OrderPlaced` and still raises on an unpaid one.

5. **Test-seam cost.** Declaring messages on the module is right for production
   code (a handler subscribed to a list not containing what it matches on becomes
   unrepresentable) and awkward for tests, where one `Echo` handler currently
   serves every message list. `test/support/handlers.ex` grows a module per
   combination.

6. **One handler, one producer — now structural.** Fan-in from two producers
   requires two modules. Unchanged in practice, but it moves from "incidental" to
   "declared", so it should be stated rather than discovered.

7. **The charset restriction is a real constraint.** `[a-z][a-z0-9_]*` on both
   halves is what buys separator-safety without escaping, and it rules out UUIDs,
   dots, and dashes in an id. Cheap, but it is a decision, not an accident.

8. **The identifier format is frozen on first deploy.** `"producer/id"` in a text
   primary key. Separator, charset, and normalization (atoms and binaries must
   render identically, so `:orders` and `"orders"` are the same identity — get
   this wrong and a config style change silently resets cursors) are all
   permanent. Hence the golden test.

9. **Nothing here changes the whole-log-then-filter-locally model.** Every
   consumer still receives and discards every message its producer publishes, and
   every producer still pays `C × P` reads. What the refactor does buy, free, is
   that the future server-side selector is now a property of a handler's
   declaration rather than an argument to a call — so it can land without adding
   an option.

---

## Verification

```
mix format --check-formatted
mix compile --warnings-as-errors
mix credo --strict
mix test                # unit; needs Postgres for :database tags
mix test.integration    # non-sandboxed; LISTEN/NOTIFY + real Bandit server
mix dialyzer
```

Postgres comes from `.devcontainer/docker-compose.yml`;
`DMEX_POSTGRES_URL` overrides. `test/test_helper.exs` skips `:database` tests
with a message when nothing is reachable — confirm they actually **ran** rather
than trusting a green summary.

End-to-end check that the identity is what it claims, via
`test/integration/vertical_slice_test.exs` as the model:

1. Start a producer, publish, subscribe a handler
   `producer: :test, id: :slice`, let it drain.
2. `SELECT subscription, last_seen FROM dmex_cursors` → one row keyed
   `"test/slice"`.
3. **Rename the handler module**, restart the subscription, publish again →
   cursor continues; no replay. This is benefit (b), directly asserted.
4. **Change the URL** in the producer map to a second server fronting the same
   log, restart → cursor continues. Benefit (a).
5. Add a clause for a second message type, restart → cursor continues, the new
   type handled from the tip onward and *not* replayed. Pins the trade in
   [Critique](#the-strongest-argument-against-the-choice-we-made).
6. Two handlers declaring the same `{producer, id}` → `DMEx.Consumer` raises at
   `init/1` naming both modules.

---

## Open, deliberately

- Should `plans/` join `docs: extras` in `mix.exs` alongside
  `guides/deployment.md`? Default answer is no — a working plan is not
  user-facing documentation.
- ~~The `@before_compile` check in Critique #4 (declared messages vs. matched
  clauses) is a separate change and not part of this refactor.~~ **Done, as a
  follow-up** — and by deriving the list from the clauses rather than checking a
  declaration against them, so there is nothing left to disagree. See Critique
  #4.
