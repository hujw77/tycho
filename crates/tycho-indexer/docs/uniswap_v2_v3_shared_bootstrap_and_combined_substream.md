# Uniswap V2/V3 Shared Bootstrap And Combined Substream Plan

## Status

- Phase 1: complete
- Phase 2: complete
- Phase 3: in progress

## Goal Lock

This document now tracks the final target architecture, not just the first combined-package
milestone.

The locked end-state goal is:

1. one shared bootstrap pipeline for the Uniswap family
2. one shared upstream Substreams session for the Uniswap family
3. protocol-specific branch materialization below that shared pipeline
4. stable external Tycho and Fynd semantics
5. extension points that let future protocols plug into the same orchestration model

Anything that still runs as:

- per-protocol bootstrap execution
- per-protocol Substreams session management
- ad hoc protocol branching in the runner

should be treated as transitional, even if it is functionally correct.

Current implementation status:

- shared bootstrap parsing has been extracted from `main.rs` into
  `crates/tycho-indexer/src/config.rs`
- bootstrap and substreams config files support recursive `includes`
- extractor top-level YAML files support recursive `includes`
- Uniswap V2/V3 bootstrap now flows through a shared bootstrap entrypoint
- Uniswap V2/V3 extractor entrypoints are now composed from shared fragments instead of
  copying full extractor blocks
- Uniswap V2 Substreams handlers now delegate into a reusable family-scoped `core.rs`
- Uniswap V3 Substreams handlers now delegate into a reusable family-scoped `core.rs`
- both Uniswap crates now gate standalone Substreams handler exports behind a feature flag,
  allowing them to be reused as Rust libraries without duplicate wasm exports
- a first combined package now builds successfully:
  `protocols/substreams/ethereum-uniswap-v2-v3-combined`
- family-level raw protobuf dispatch now exists in
  `crates/tycho-indexer/src/extractor/family_dispatch.rs`
- family-level bootstrap planning and merged bootstrap materialization now exist in
  `crates/tycho-indexer/src/extractor/shared_bootstrap.rs`
- bootstrap strategy parsing and branch materialization are centralized in
  `crates/tycho-indexer/src/extractor/bootstrap_registry.rs`
- a family runtime registry now exists in
  `crates/tycho-indexer/src/extractor/family_runtime.rs`
- `tycho-indexer` can now detect the combined Uniswap family config and build one shared
  upstream Substreams session with protocol-specific downstream branch extractors
- combined extractor fragments now declare their family runtime explicitly, so single-stream
  orchestration no longer depends on matching a hard-coded shared `.spkg` filename pattern
- family runtime detection is now explicit-only as well: members enter the shared family path by
  declaring `family_runtime`, not by implicitly sharing a package path that happens to match a
  naming convention
- explicit family opt-in now also requires a complete member set: once any protocol in a family
  declares `family_runtime`, the repo must provide exactly one extractor config for every declared
  family member instead of silently degrading back to standalone execution
- family-level shared stream settings can now be declared once at the extractor-config top level
  and inherited by member extractors, instead of repeating `shared_spkg` and `shared_module`
  in every branch fragment
- combined-family member fragments no longer need to repeat the shared `.spkg` path either:
  when `family_runtime.shared_spkg` is resolved from the top-level family config, member
  extractor configs can omit `spkg` and still build the correct family runtime plan
- family-level shared stream settings now also cover the stream boundary itself:
  `family_runtimes.<family>.stop_block` can be declared once at the top level and inherited by
  member extractors unless a branch explicitly overrides it
- shared family stream planning now enforces that `stop_block` resolves to one effective value
  across all family members; conflicting member-level values fail setup instead of silently
  widening the shared stream boundary with `max(stop_block)`
- family runtime resolution now also enforces aligned effective fresh-start blocks before runner
  construction: if bootstrap-adjusted `start_block` values diverge across family members, the
  shared runtime plan is rejected at planning time instead of deferring that mismatch to runner
  startup
- family runtime resolution now also rejects incompatible merged `substreams_params` at planning
  time, so family members cannot drift on shared stream module parameters and only discover the
  conflict when the runner tries to assemble one combined request
- family member identity checks now also live at runtime-planning scope rather than only in the
  runner: exact protocol membership, chain alignment, explicit family alignment, and non-empty
  `protocol_types` are validated while resolving family configs, and the runner reuses that same
  validation surface instead of owning a separate copy
- family-level config convergence now also covers protocol-scoped member defaults under the
  shared runtime: `family_runtimes.<family>.members.<protocol_system>.substreams_params` can now
  centralize combined-family module params at the top level, allowing combined V2/V3 configs to
  remove repeated per-fragment `substreams_params` blocks while still materializing the correct
  branch-local filters for each protocol member
- those planning-time shared-stream constraints are now also wired through the real combined
  entrypoint: `build_all_extractors(...)` has direct regression coverage proving a conflicting
  family `stop_block` fails before runner build or member package loading begins
- the real combined entrypoint also now has a positive family-defaults coverage path:
  `build_all_extractors(...)` can successfully build one shared family runner from a config that
  omits member-level `spkg` and `stop_block`, inheriting both from top-level
  `family_runtimes.uniswap` defaults instead
- the family runner now resolves and loads the runtime package from the detected family-level
  `shared_spkg`, instead of implicitly reusing the first member extractor's package path
- extractor configs can now declare `protocol_system` explicitly, and family-runtime resolution
  no longer depends on extractor config keys or `name` matching the protocol identity exactly
- shared route filtering now keys off `protocol_system` for family-enabled extractors, so aliased
  extractor ids do not break protocol-specific bootstrap or substreams pool selection
- the shared family dispatcher is pre-seeded from the protocol cache, so resumed streams can
  route updates for components that were created before the current process started
- the shared family dispatcher now also pre-seeds contract-address ownership from the protocol
  cache and learns new component contracts at admission time, allowing storage-only and
  contract-only follow-up updates to stay routable under the shared stream path
- the shared Substreams stream path now has direct reconnect coverage: after a streamed block is
  followed by a gRPC error, the next request resumes from the latest cursor rather than the
  original start block
- the family runner now also has reconnect coverage above that stream layer: after reconnect, the
  dispatcher still routes follow-up updates for previously created family components into the
  correct protocol branches
- the family runner also has restart-style routing coverage: when component ownership is
  pre-seeded from cached protocol state, follow-up updates route correctly even if the current
  process never replayed the original component-creation block
- that restart-style coverage now also includes contract-only and storage-only follow-up updates
  via pre-seeded contract-address ownership, not just entity/component follow-ups
- the startup preload path is now covered one layer closer to production as well: the shared
  dispatcher can be built from a `ProtocolMemoryCache` that was populated through the gateway,
  proving the cache/DB seed path and the manual in-memory seed path behave the same for resumed
  family follow-up routing
- dispatcher preload ownership now also lives with the family-dispatch layer itself: family
  runners only provide branch specs plus a protocol cache, while component/contract ownership
  seeding is derived and applied through `family_dispatch.rs`, which keeps future family runtimes
  from re-implementing cache preload logic in each runner path
- runner shutdown semantics are now aligned with the shared-runtime design as well: both the
  single-extractor runner and the family runner flush buffered finalized blocks before treating
  an `Ended` signal as terminal, so the last committed family updates are not stranded in the
  reorg buffer at normal stream shutdown
- dynamic component admission is covered in the dispatcher: once a family block creates a new
  component, later updates for that component route to the same protocol branch
- dynamic family admission is now also covered one layer above the dispatcher: a DB-backed
  combined-family test proves that a newly created Uniswap V2 component carried by the shared
  family stream is persisted through the real extractor/gateway path and becomes queryable from
  storage after the next block advances the commit boundary
- that same DB-backed combined-family test now reaches the public RPC surface too: after the
  shared family runner persists the dynamically admitted component, a standalone Tycho RPC server
  can return it through `/v1/protocol_components`, which is the strongest proof so far that the
  shared runtime still preserves external component-discovery semantics
- the same external-semantics regression now also covers `/v1/protocol_state`: the dynamically
  admitted family pool carries a minimal state delta through the shared runner/extractor path and
  is readable through the public protocol-state API without requiring per-protocol stream
  orchestration
- the combined Substreams package now emits family transaction and storage changes in
  deterministic transaction-index order
- combined-family Substreams crates now also expose pure Rust wrapper entrypoints for their
  handler semantics, so shared-runtime regressions can exercise the same created-pool,
  follow-up-event, and family-merge logic as the combined package without depending on wasm ABI
  shims inside unit/integration tests
- family runtime detection now goes through a registry abstraction, so future protocol families
  can be added by registering family specs without changing the runner orchestration
- resolved runtime planning now also exposes one unified runtime-target surface for the indexer
  entrypoint, so `main.rs` no longer needs separate family and standalone planning passes before
  it starts building runners
- family runtime registration now also owns the shared-bootstrap branch metadata for each member
  protocol, so stream-family membership and bootstrap-family membership no longer drift through
  separate hard-coded member lists
- family member registration now models shared-bootstrap support as one atomic capability object
  instead of three loosely related optional fields, so future protocol families cannot represent
  partial bootstrap handler declarations that only fail much later during runtime setup
- shared bootstrap materialization is now dispatched through the family runtime registry as well:
  `shared_bootstrap.rs` builds the plan, but the family spec owns the family-level execution
  entrypoint, so shared bootstrap orchestration is no longer hard-coded as a generic
  branch-by-branch loop outside the family runtime model
- shared bootstrap parameter semantics are now partially lifted to the registry too: the common
  `bootstrap_block + pool(s)` shape is parsed once through a family-level shared parser instead
  of wiring near-identical V2/V3 parser callbacks, while still preserving an explicit custom
  parser extension point for future families that need a different bootstrap grammar
- built-in family declarations are now being pulled out of the core runtime planner as well:
  the default Uniswap family registration lives in a dedicated registry module instead of being
  hard-coded inline with the family-planning logic, which narrows the core runtime surface that
  needs to change when a new built-in family or member protocol is introduced
- Uniswap-specific bootstrap materialization has been pushed another step out too:
  the V2/V3 branch materializers and the Uniswap family-level merged bootstrap materializer now
  live in a family-specific module instead of the generic family planner, so the core runtime no
  longer needs direct protocol imports just to support the default built-in family
- shared bootstrap planning and bootstrap-registry lookup now expose registry-parameterized entry
  points as well, so future families can reuse the same bootstrap-plan construction path under a
  custom family registry instead of being forced through the built-in Uniswap registry
- shared bootstrap branch parsing/materialization no longer travels through a separate
  `bootstrap_registry.rs` indirection layer: branch-level bootstrap capability resolution now sits
  directly on `family_runtime.rs`, so family detection, shared-bootstrap planning, branch parsing,
  and family-level materialization all consult the same registry surface
- shared runtime metadata resolution is now converging on that same registry too:
  `family_runtime.shared_spkg/shared_module` inheritance is resolved and validated through a
  registry entrypoint instead of being manually stitched together inside `config.rs`
- shared bootstrap planning now validates the family registry at the plan-construction entrypoint,
  so incomplete custom-family bootstrap declarations fail before any branch parsing or
  materialization begins
- shared bootstrap planning now rejects mixed inferred families even when member extractors do not
  explicitly declare `family_runtime`, closing a configuration hole where unrelated protocol
  systems could otherwise enter the same shared bootstrap plan
- shared bootstrap splitting is now closer to full family fidelity as well: protocol-system
  demultiplexing no longer drops `block_contract_changes` or `trace_results`, so family-level
  bootstrap materialization can carry DCI-relevant contract changes and trace outputs through the
  same shared split/apply path instead of relying on an explicit unsupported-field guard
- shared bootstrap durability is now family-scoped as well: combined-family branches no longer
  persist separate `extractor_name::bootstrap` completion markers during the shared bootstrap
  path; instead they share one family-scoped bootstrap checkpoint while still falling back to the
  legacy per-extractor marker during migration/resume
- fresh shared-family startup now also enforces bootstrap coherence: a family run cannot mix
  fresh branches that declare bootstrap with fresh branches that omit it, because that would
  silently reintroduce per-branch bootstrap semantics into the shared family path
- that bootstrap coherence is now also validated during family-runtime planning, so invalid
  mixed-bootstrap family configs fail before runner construction instead of only surfacing during
  startup
- those same family-runtime invariants are now enforced at config-load time too:
  explicit family opt-in with a missing declared member extractor, or a mixed shared-bootstrap
  family config, fails while loading the YAML instead of surviving into runtime setup
- remaining work is concentrated in runtime hardening and verification, not in basic single-stream
  plumbing
- partitioned storage writes are now hardened for restart/resume paths as well: when a follow-up
  update archives an old `protocol_state`, `component_balance`, or `contract_storage` row whose
  `valid_to` lands on an unpremade historical day, Tycho now creates the required daily partition
  on demand before inserting the archive row, preventing it from falling back into the default
  partition and colliding with the live-row uniqueness constraints during shared-runner restarts

## Context

Today, Uniswap V2 and Uniswap V3 run as two independent extractors inside the same
`tycho-indexer` process.

- V2 extractor config points to `ethereum-uniswap-v2-v0.3.2.spkg`
- V3 extractor config points to `ethereum-uniswap-v3-logs-only-v0.1.2.spkg`
- V2 bootstrap and V3 bootstrap are separate RPC bootstrap paths
- V2 and V3 maintain separate Substreams sessions, cursors, and recovery behavior

This separation keeps failure domains small, but it also duplicates configuration,
bootstrap wiring, and runtime coordination. Recent debugging exposed one concrete cost:
the single-protocol extractor config had the V2 `substreams_params` fix, while the
combined V2+V3 extractor config did not, causing most bootstrapped V2 pools to stay at
bootstrap-only state.

## Current Problems

### 1. Config drift

The same logical V2 bootstrap wiring had to be duplicated in:

- `extractors.uniswap_v2.yaml`
- `extractors.uniswap_v2_v3.yaml`

This drift caused the V2 bootstrap metadata to be passed in one runtime path but not
the other.

### 2. Bootstrap knowledge is protocol-local

V2 and V3 each carry their own bootstrap source of truth and parameter expansion path.
That means:

- duplicate route parsing
- duplicate pool metadata derivation
- duplicate start-block coordination

### 3. Runtime duplication

V2 and V3 both:

- open separate Substreams sessions
- maintain separate cursors
- consume overlapping chain ranges
- reconnect independently

This is not incorrect, but it is operationally heavier than necessary.

## Goals

1. Eliminate bootstrap/config drift between V2-only and V2+V3 deployments.
2. Keep Tycho RPC semantics stable for downstream consumers such as Fynd.
3. Reduce repeated configuration parsing and Substreams setup work.
4. Replace per-protocol RPC bootstrap execution with one shared bootstrap pipeline for the
   Uniswap family.
5. Replace per-protocol Substreams sessions with one shared Substreams stream for the Uniswap
   family.
6. Preserve extensibility so new protocols can plug into the same bootstrap and stream
   orchestration model without duplicating coordination code.

## Non-Goals

1. Changing Tycho RPC response formats.
2. Merging `protocol_system` identities exposed to clients.
3. Rewriting Fynd integration logic.
4. Unifying V2/V3 simulation or decoding logic.
5. Sacrificing protocol-local state semantics at the API boundary just to collapse internal
   orchestration.

## Updated Recommendation

The original three phases were useful to de-risk the first combined package, but they are no
longer the desired end state.

The next-phase target architecture should be:

1. one shared bootstrap pipeline for the Uniswap family
2. one shared Substreams session for the Uniswap family
3. protocol-specific branching below that shared pipeline
4. stable downstream Tycho/Fynd semantics identical to today's externally visible behavior

This means the remaining work should no longer optimize for "optional combined mode while
keeping separate extractor sessions forever". It should optimize for converging on a genuinely
shared runtime that still preserves protocol-local state, filtering, and downstream identities.

## Target Architecture

The intended end state is:

```text
shared bootstrap config
  -> shared bootstrap planner
  -> shared bootstrap executor
  -> shared seed state for protocol branches

shared substreams package
  -> single shared stream session
  -> shared block dispatcher
  -> protocol-family branches (v2, v3, later others)
  -> protocol-specific state materialization
  -> stable Tycho RPC surfaces
```

### Shared bootstrap pipeline

The bootstrap path should become one orchestrated pipeline with the following stages:

1. load one family-level bootstrap config
2. derive protocol membership and route inventory
3. collect required on-chain metadata for all configured pools
4. materialize protocol-specific bootstrap state from one shared execution pass
5. persist one shared bootstrap checkpoint plus protocol-specific derived state

This removes the current duplication where V2 and V3:

- parse the same family intent separately
- perform separate RPC bootstrap coordination
- maintain separate bootstrap completion paths

### Shared stream pipeline

The stream path should become one orchestrated runtime with the following stages:

1. open one Substreams session against one package and one output module
2. receive one family-level block payload
3. dispatch changes to protocol-family branch handlers
4. update protocol-specific stores and Tycho state
5. maintain stable `protocol_system` identities at the API boundary

The key point is that "one package with two separately subscribed modules" is not the final
target. The final target is one upstream stream plus downstream branching.

### Extensibility requirements

The architecture should not hard-code "Uniswap V2 and V3" as a closed set. It should expose
clear extension points for future protocols. In practice this means:

1. bootstrap discovery should be expressed in terms of protocol-family planners and protocol
   branch descriptors
2. stream demultiplexing should dispatch into protocol branch handlers through an interface,
   not through ad hoc if/else orchestration
3. family-level orchestration should be reusable when adding another protocol that belongs in
   the same shared runtime domain
4. adding a new protocol should primarily require:
   - a branch decoder/materializer
   - protocol-specific bootstrap data collection logic where needed
   - registration into the shared family plan
   not a brand-new orchestration path

## Phase 1: Shared Bootstrap

Status: complete

### What changes

Introduce one canonical bootstrap source for the Uniswap family, conceptually:

- shared start block
- shared route inventory
- explicit per-router protocol
- optional per-protocol overrides

Example shape:

```yaml
start_block: 25379140
routes:
  - token0: "..."
    token1: "..."
    routers:
      - pool: "..."
        protocol: uniswap_v2
      - pool: "..."
        protocol: uniswap_v3
```

### Execution model

The shared config is expanded into protocol-specific outputs:

- V2 bootstrap params:
  - `bootstrap_block`
  - `pools`
  - `pool_tokens`
- V3 bootstrap params:
  - `bootstrap_block`
  - V3 pool list
  - any V3-specific parameters such as factory routing

### Required code changes

1. Move route parsing and filtering into one shared helper.
2. Filter by `router.protocol` before generating protocol-specific params.
3. Generate both:
   - extractor bootstrap params
   - substreams module params
   from the same parsed object.

### Benefits

- removes config drift between V2-only and V2+V3 configs
- makes protocol membership explicit
- prevents accidental cross-protocol pool injection
- keeps runtime architecture unchanged

### Risks

- low
- mostly limited to config parsing regressions

### Landed Implementation

- added shared bootstrap normalization and protocol-aware route filtering in
  `crates/tycho-indexer/src/config.rs`
- added shared bootstrap entrypoint
  `crates/tycho-indexer/config/shared_uniswap_bootstrap.yaml`
- V2 substreams params now flow through
  `crates/tycho-indexer/config/uniswap_v2_substreams.yaml`
- added regression coverage for:
  - V2/V3 route filtering
  - start block consistency
  - repo-level bootstrap parity

## Phase 2: Shared Extractor Composition

Status: complete

### What changes

Keep separate V2 and V3 extractors, but generate or compose them from shared bootstrap
logic instead of hand-copying config.

Possible approaches:

1. Static YAML composition:
   - one shared YAML fragment
   - protocol-specific overlays
2. Rust-side config expansion:
   - load one shared bootstrap description
   - synthesize per-extractor params in `main.rs`

### Recommendation

Prefer Rust-side expansion because the project already centralizes bootstrap parameter
normalization in `tycho-indexer/src/main.rs`.

### Benefits

- preserves independent sessions and cursors
- eliminates duplicated V2/V3 bootstrap param wiring
- simpler to validate than a full combined substream

### Risks

- moderate
- mostly around rollout correctness rather than runtime behavior

### Landed Implementation

- added top-level extractor config composition via recursive `includes` in
  `crates/tycho-indexer/src/config.rs`
- introduced shared extractor fragments:
  - `crates/tycho-indexer/extractors.fragments/uniswap_v2.yaml`
  - `crates/tycho-indexer/extractors.fragments/uniswap_v3_protocol_changes.yaml`
  - `crates/tycho-indexer/extractors.fragments/uniswap_v3_events.yaml`
- converted real entrypoints to composition:
  - `crates/tycho-indexer/extractors.yaml`
  - `crates/tycho-indexer/extractors.uniswap_v2.yaml`
  - `crates/tycho-indexer/extractors.uniswap_v2_v3.yaml`
- added regression coverage for:
  - extractor top-level include loading
  - repo-level V2 entrypoint parity
  - repo-level V3 entrypoint parity

## Phase 3: Shared Runtime Convergence

Status: started

### What changes

Converge the current intermediate combined package work into a true family-level runtime:

1. shared bootstrap execution
2. one shared Substreams stream
3. protocol-specific branch materialization below that stream

Conceptually:

```text
source block
  -> shared family output
  -> branch dispatcher
  -> V2 branch materializer
  -> V3 branch materializer
  -> protocol-specific state updates
```

The important constraint is that Tycho should still expose stable downstream identities:

- `uniswap_v2`
- `uniswap_v3`

Even if upstream execution is unified, the API-facing semantics should remain stable.

### Required end state

The target architecture now explicitly requires:

1. one upstream Substreams session per family runtime
2. one shared family-level output contract from Substreams into the indexer
3. downstream branch handlers that preserve protocol-local state semantics
4. shared bootstrap execution instead of per-protocol bootstrap runners

An intermediate "same package, still two separate subscriptions" model may still be used during
migration, but it should be treated as a stepping stone rather than the destination.

### Phase 3 Spike Result

An initial spike confirmed one important implementation constraint:

- existing Substreams handler exports cannot be reused as thin Rust wrappers across crates

Reason:

- `#[substreams::handlers::map]` and `#[substreams::handlers::store]` transform exported
  functions into FFI-style entrypoints
- those generated entrypoints are suitable for Substreams runtime loading, but not for normal
  in-process Rust composition
- a naive "combined crate depends on V2/V3 crates and simply calls their handlers" approach does
  not compile

This means Phase 3 should not proceed with a thin-wrapper design.

### Phase 3 Progress Update

The core Phase 3 runtime architecture is now in place:

- `protocols/substreams/ethereum-uniswap-v2` exposes reusable pure logic through
  `src/core.rs`
- `protocols/substreams/ethereum-uniswap-v3-logs-only` exposes reusable pure logic through
  `src/core.rs`
- protocol-specific Substreams handler entrypoints remain in place, but they are now thin
  wrappers over reusable Rust functions
- both protocol packages now build as `cdylib + rlib`, making them suitable as future library
  dependencies for a combined package
- standalone handler exports are now isolated behind a `standalone-handlers` feature so the
  combined crate can depend on the V2/V3 crates without wasm symbol collisions
- a first combined crate now exists and passes `cargo test --no-run` and
  `substreams build --manifest ethereum-uniswap-v2-v3.yaml`
- V3 runtime filtering has now been adjusted toward a seed-plus-dynamic-admission model instead
  of a permanent bootstrap allowlist
- the combined Substreams package now exposes a family-level merged output module
  `map_uniswap_family_protocol_changes`
- indexer-side shared bootstrap logic now supports family-level planning, merged materialization,
  split-once application, and branch-progress consistency checks
- indexer-side raw-protobuf family dispatching is now wired into a `FamilyExtractorRunner`, so
  one shared upstream Substreams session fans out into protocol-local downstream extractors
- family runtime detection and resolution now live behind explicit family-level interfaces in
  `family_runtime.rs`

This means the codebase is no longer in the earlier "combined package exists but runtime is still
per protocol" state. The shared bootstrap path and single shared stream path both now exist in the
indexer for the detected Uniswap family.

### Remaining Architecture Work

The remaining Phase 3 work is now concentrated in hardening, validation, and future-family
extensibility rather than in first-principles orchestration:

1. validate resume, reconnect, restart, and cursor behavior on the shared family path under real
   combined-indexer runs
2. preserve dynamic factory pool admission under the shared runtime, especially after bootstrap
3. continue reducing places that rely on implicit assumptions such as
   `extractor name == protocol_system`
4. make family registration and shared bootstrap registration easier to extend for future protocol
   families without re-opening runner-level branching
5. continue deciding which additional settings truly belong at the family level beyond the now
   shared `shared_spkg`, `shared_module`, `bootstrap`, and `stop_block` fields, such as
   family-scoped route-filter defaults

### Family Registration Model

The current code now converges on a stricter family registration shape:

1. a family spec declares the family name, shared package hint, shared output module, and member
   protocol set
2. each family member declaration also carries its shared-bootstrap metadata:
   - `protocol_system`
   - bootstrap strategy
   - bootstrap param parser
   - bootstrap branch materializer
3. shared stream detection and shared bootstrap routing both consult the same family-member
   registration source
4. family registration is now validated before runtime planning, so duplicate member protocol ids
   or incomplete bootstrap handler declarations fail early during setup instead of surfacing later
   on one shared execution path
5. shared bootstrap planning also re-validates that registry at the entrypoint where custom
   registries are consumed, so future-family callers cannot bypass those invariants accidentally

This is an important extensibility improvement because adding another protocol to an existing
family no longer requires updating:

- one member list for stream-family detection
- another separate member list for bootstrap-family routing

Instead, the intended path is to register one new family member descriptor and let both runtime
detection and shared bootstrap resolution derive from that same declaration.

What is complete today:

1. shared config and package groundwork
2. one family-level output contract from Substreams
3. one shared family stream runner in the indexer
4. one shared bootstrap executor with merged materialization and per-protocol downstream apply
5. family-runtime planning interfaces that separate family orchestration from standalone extractors

What still needs more confidence:

1. production-like restart and resume behavior
2. reconnect behavior after upstream failures
3. dynamic pool admission after the shared bootstrap seed set
4. extension ergonomics when a new protocol joins an existing family or a new family is added
5. factory-created pool discovery driven by the combined Substreams package instead of only
   synthetic admission fixtures

### Shared Stream Constraint Discovered

One important constraint is now explicit in the code:

1. the family-level `BlockChanges` protobuf output does not carry `protocol_system` per component
2. the current indexer `TryFromMessage` path injects one configured `protocol_system` for the
   whole decoded payload
3. therefore, a true shared stream cannot simply decode the merged payload through one existing
   per-protocol extractor path

The required runtime direction is:

1. receive one family-level raw protobuf payload from Substreams
2. dispatch that raw payload into per-protocol branch payloads using protocol-type and
   component-membership routing
3. only then decode each branch payload through the existing protocol-local extractor logic

### Execution Plan From Here

The remaining implementation should proceed in this order:

1. keep the shared family runner as the primary convergence path
2. validate resume, cursor, restart, and reconnect behavior under the shared path
3. prove dynamic admission still works on top of the shared bootstrap seed model
4. continue extracting family registration seams so future protocols plug in by registration,
   not by new runner branches
5. only after that, consider removing transitional legacy runtime paths

This order is important because it preserves correctness first, then converges the runtime
surface, then removes duplicated orchestration.

### Next Slice: Shared Bootstrap + Dynamic Admission

The next major gap is no longer "can we share bootstrap and stream orchestration". That part now
exists. The next gap is proving that the shared runtime keeps the same correctness properties once
dynamic admission and real operational behavior are layered on top.

The next follow-up goals should therefore be pursued together:

1. dynamic factory pool admission must continue to work under the genuinely shared bootstrap model
2. shared family restart and resume behavior must stay coherent across all member branches

#### Scope

1. keep bootstrap route filtering as the initial seed set for V2 and V3
2. execute seed collection through one shared bootstrap pipeline
3. continue listening to factory `PoolCreated` or equivalent creation events after bootstrap
4. materialize newly discovered pools into Tycho state automatically
5. ensure downstream event modules begin accepting updates for those newly admitted pools
   without requiring a manual bootstrap config change
6. preserve protocol-aware filtering so V2 and V3 branches do not ingest each other's pools

#### Design Constraints

1. bootstrap configuration should define the initial synchronization scope, not act as a hard
   forever-allowlist unless explicitly configured that way
2. dynamic admission must not regress the recent fix that prevents runtime processing of foreign
   or not-yet-known pools
3. newly discovered pools must become visible through the same Tycho RPC surfaces:
   `protocol_components`, `protocol_state`, and protocol component state snapshots
4. unified stream execution must not break downstream protocol-local ordering and state semantics

#### Current DCI Constraint

One runtime limitation is still worth keeping explicit:

1. `storage_changes` only carry transaction-level storage deltas keyed by contract address, not
   protocol component ids
2. the current family dispatcher therefore routes storage changes by first inferring which
   protocol branch matched the rest of the transaction
3. this is sufficient for the current shared Uniswap path, but a future family-level DCI design
   will need a stronger routing contract if storage-only transactions must be supported across
   multiple protocol branches
5. the abstractions introduced here must be reusable for future protocols in the same runtime
   family

#### Acceptance Criteria

1. starting from the shared bootstrap seed set, the extractor later ingests a newly created V2
   pool without editing bootstrap YAML
2. starting from the shared bootstrap seed set, the extractor later ingests a newly created V3
   pool without editing bootstrap YAML
3. newly admitted pools receive follow-up state updates, not just creation records
4. Tycho RPC exposes the new pools through `protocol_components`
5. Fynd can route through a dynamically admitted pool once it becomes relevant

#### Recommended Rollout

1. validate the current shared runtime on real combined-indexer runs
2. then converge dynamic admission semantics across V2 and V3 under that shared runtime
3. then generalize family registration and shared bootstrap registration for future protocols
4. finally remove transitional code paths once the shared path has enough operational confidence

This preserves:

- standalone V2 package
- standalone V3 package
- combined package

while avoiding direct reuse of macro-transformed handler entrypoints.

## Downstream Compatibility With Fynd

Fynd should remain unaffected if these invariants hold:

1. `protocol_system` remains `uniswap_v2` and `uniswap_v3`
2. component ids stay unchanged
3. protocol_state and protocol_component RPC semantics stay unchanged
4. websocket delta ordering remains internally consistent per extractor

Fynd does not need to know whether Tycho used:

- two packages
- one package with two modules
- one package with a shared pre-processing pipeline

It only depends on the external Tycho API contract.

## Risk Comparison

### Shared bootstrap only

- lowest risk
- highest immediate ROI
- directly addresses the configuration drift that caused the recent V2 issue

### Shared extractor composition

- low to moderate risk
- good operational payoff
- keeps failure domains separate

### Combined substream

- highest implementation and regression risk
- best long-term runtime simplification only if maintained carefully
- should be deferred until bootstrap/config unification is stable

## Proposed Implementation Order

1. Create a shared Uniswap family bootstrap schema.
2. Add protocol-aware filtering when expanding routes into extractor params.
3. Make `extractors.uniswap_v2.yaml` and `extractors.uniswap_v2_v3.yaml` consume the same
   bootstrap expansion path.
4. Add tests that assert the V2-only and V2+V3 configs both produce identical V2
   `substreams_params`.
5. Add the same parity tests for V3.
6. Only then evaluate a combined package with separate V2/V3 output modules.

## Validation Checklist

### Shared bootstrap rollout

- V2-only config and V2+V3 config expand to identical V2 params
- V3-only config and V2+V3 config expand to identical V3 params
- bootstrap pool counts match expected route counts per protocol
- protocol filtering excludes foreign pools from each protocol branch

### Runtime correctness

- bootstrapped V2 pools continue receiving post-bootstrap `Sync` updates
- bootstrapped V3 pools continue receiving post-bootstrap tick/liquidity updates
- RPC `protocol_state` matches chain state at the tested block
- Fynd E2E quote passes for:
  - V2-only
  - V3-only
  - V2+V3

### Combined substream validation

- repo-level combined extractor config builds exactly one Uniswap family runtime plan
- cursor resume works independently for both logical extractors
- reorg handling preserves extractor-local revert semantics
- V2 branch failure does not corrupt V3 persisted state, and vice versa
- factory-discovered pools are admitted after bootstrap and continue receiving state updates
- combined mode does not treat bootstrap pools as a permanent allowlist unless configured to do so

## Completion Audit Snapshot

This snapshot is stricter than the phased implementation notes above. Its purpose is to separate
requirements that are directly proven by current code/tests from requirements that are still only
partially evidenced or still missing a dedicated regression.

### Directly Proven

- one shared bootstrap planning path exists for Uniswap-family members and is exercised by
  `SharedBootstrapPlan` tests as well as the family-runner bootstrap path
- combined-family config no longer needs duplicated per-extractor bootstrap params:
  top-level `family_runtimes.<family>.bootstrap.params` now fans out through member-specific
  shared-bootstrap strategy resolution, and
  `extractor_config_inherits_family_bootstrap_defaults_from_top_level` proves the repo can
  express one family-level bootstrap source of truth while still materializing the right
  protocol-specific branch strategy
- one shared upstream Substreams session is used for the combined Uniswap family:
  `combined_config_builds_one_family_runner` and
  `combined_family_runner_resumes_from_persisted_branch_progress` both prove a single shared
  family runner / single upstream request path
- the combined Substreams package itself directly preserves family-level merge semantics:
  `merges_v2_and_v3_changes_into_one_family_block`,
  `merged_family_block_preserves_transaction_index_order`,
  `merged_family_block_preserves_storage_change_transaction_index_order`, and
  `merged_family_block_preserves_all_change_vectors_for_same_transaction_hash`
  prove that the shared package emits one merged family block while preserving tx ordering,
  storage ordering, and same-tx aggregation of component, entity, balance, and contract changes
- runtime branch dispatch below the shared stream is directly covered for:
  component creation, entity updates, contract-only follow-ups, storage-only follow-ups,
  reconnect, restart-style cache preload, and end-of-stream flushing
- runner-level cross-branch failure isolation is directly covered:
  `test_family_runner_does_not_propagate_partial_branch_results_when_later_branch_fails`
  proves a later failing family branch does not leak earlier branch results to subscribers for
  the same shared-stream block
- persistence-level cross-branch failure isolation is directly covered:
  `test_family_runner_does_not_durably_persist_failing_block_across_branches`
  proves an earlier successful branch block can become durable while a later shared-family block
  that fails in another branch does not leave partial component/state/cursor persistence behind
- DB-backed shared-family revert semantics are now also directly covered across multiple protocol
  branches:
  `combined_family_runner_reverts_dynamically_admitted_components_across_branches`
  proves that once a shared-family block has durably admitted both a V2 and a V3 component,
  a later `BlockUndoSignal` can remove both branches' component/state visibility from storage and
  from `/v1/protocol_components` and `/v1/protocol_state`
- DB-backed shared-family reorg recovery semantics are now also directly covered across multiple
  protocol branches:
  `combined_family_runner_recovers_after_revert_and_reapplies_multi_branch_state`
  proves that after such a shared-family revert, the same single upstream family stream can ingest
  the new canonical branch for both V2 and V3, re-materialize both components, persist later
  follow-up state updates, and expose that recovered post-reorg state through both direct storage
  reads and `/v1/protocol_components` and `/v1/protocol_state`
- dynamic admission is directly covered through the real extractor/gateway path:
  `combined_family_runner_persists_dynamically_admitted_component`
  proves that a newly admitted family pool is persisted and externally queryable
- dynamic follow-up state after admission is directly covered at the latest-view storage path:
  `combined_family_runner_persists_follow_up_state_for_dynamically_admitted_component`
  proves that a pool admitted through the shared family stream can receive a later state update
  and that the latest storage view, explicit timestamp-version storage path, and RPC default
  timestamp path all expose the updated attribute value
- seeded-universe plus dynamic factory-style onboarding is directly covered:
  `combined_family_runner_factory_style_dynamic_component_joins_seeded_universe_and_receives_follow_up_state`
  proves that a shared-bootstrap-seeded family universe can accept a newly arriving pool on the
  shared stream and keep serving both the seeded component and the newly joined component with
  correct follow-up state persistence
- shared-family restart semantics are now also covered after dynamic admission:
  `combined_family_runner_restart_resumes_branch_progress_after_dynamic_component_admission`
  proves that once a dynamically discovered pool is admitted through the shared stream, a fresh
  process restart resumes the shared family at the next block instead of treating untouched
  branches as fresh and failing family-progress alignment
- external Tycho API semantics have direct combined-family evidence at the component/state level:
  the same DB-backed regression verifies `/v1/protocol_components` and `/v1/protocol_state`
  for a dynamically admitted pool under the shared runtime
- the shared-runtime dynamic-admission proof now exercises a real V2 creation-build path as well,
  not just a hand-authored final family protobuf payload:
  `combined_family_runner_factory_style_dynamic_component_joins_seeded_universe_and_receives_follow_up_state`
  now feeds the family runner from a block that is first transformed through the actual
  `PairCreated -> build_pool_created_block_changes -> build_uniswap_family_protocol_changes`
  construction path before entering the shared-stream boundary
- that same V2 factory-style proof now uses the real follow-up event builder too, not a
  handwritten post-admission family payload:
  `combined_family_runner_factory_style_dynamic_component_joins_seeded_universe_and_receives_follow_up_state`
  now routes the second block through
  `Sync -> build_pool_event_block_changes -> build_uniswap_family_protocol_changes`
  before asserting latest-state persistence under the shared runtime
- that V2 follow-up path no longer relies on handcrafted `pool_tokens=` admission hints in the
  regression harness either:
  both `combined_family_runner_factory_style_dynamic_component_joins_seeded_universe_and_receives_follow_up_state`
  and `combined_family_runner_restart_applies_follow_up_state_after_dynamic_component_admission`
  now seed the V2 event builder from a mock `StoreGet<ProtocolComponent>` populated by the real
  prior `PairCreated` output, matching the production contract that later blocks discover the pool
  through store-backed component lookup rather than through test-only bootstrap token injection
- the shared-runtime dynamic-admission proof now also exercises the real V3 creation-build path,
  not just a synthetic final family payload:
  `combined_family_runner_v3_dynamic_component_from_real_pool_created_block_receives_follow_up_state`
  now feeds the family runner from a block that is first transformed through the actual
  `PoolCreated -> build_v3_pool_created_block_entity_changes -> build_v3_protocol_changes ->
  build_uniswap_family_protocol_changes`
  construction path before entering the shared-stream boundary
- that same V3 dynamic-admission proof now also uses a real V3 follow-up event path, not a
  handwritten `tick` update payload:
  `combined_family_runner_v3_dynamic_component_from_real_pool_created_block_receives_follow_up_state`
  now routes the next block through
  `Swap log -> build_pool_events -> build_protocol_changes -> build_uniswap_family_protocol_changes`
  before asserting persisted latest-state visibility
- shared-family restart semantics now also have the same real-creation-path coverage for V3 as
  for V2:
  `combined_family_runner_restart_applies_v3_follow_up_state_after_dynamic_component_admission`
  proves a pool admitted through the real V3 `PoolCreated -> family block` path survives a fresh
  process restart, resumes from the next shared-family block, persists a later follow-up
  state update under the shared runtime, and remains queryable through
  `/v1/protocol_components` and `/v1/protocol_state` after that restart
- that V3 restart regression now also uses the real follow-up `Swap` event path rather than a
  handwritten family payload:
  `combined_family_runner_restart_applies_v3_follow_up_state_after_dynamic_component_admission`
  resumes from a real
  `Swap log -> build_pool_events -> build_protocol_changes -> build_uniswap_family_protocol_changes`
  block and verifies both storage and RPC visibility after restart
- shared-family restart semantics now also have real V2 follow-up-path coverage instead of a
  handwritten restart payload:
  `combined_family_runner_restart_applies_follow_up_state_after_dynamic_component_admission`
  now resumes from the real
  `Sync -> build_pool_event_block_changes -> build_uniswap_family_protocol_changes`
  path after restart, proving dynamic admission, persisted branch progress, and follow-up state
  routing all stay coherent under the shared stream
- those real V2 factory/discovery regressions are also now verified end-to-end through the
  DB-backed shared runner itself:
  both
  `combined_family_runner_factory_style_dynamic_component_joins_seeded_universe_and_receives_follow_up_state`
  and
  `combined_family_runner_restart_applies_follow_up_state_after_dynamic_component_admission`
  execute successfully under the `tycho-indexer` binary test harness, confirming the combined
  handler wrappers, shared runner, storage path, and restart semantics agree on the same
  production-facing contract
- Fynd-side combined-family builder/feed wiring is directly covered:
  `assemble_components_propagates_combined_uniswap_protocols_to_tycho_feed`
  proves the explicit `uniswap_v2 + uniswap_v3` protocol list is forwarded unchanged through
  Fynd's solver builder into `TychoFeedConfig`
- Fynd-side combined-family feed consumption semantics now also have automatic proof:
  `test_handle_message_tracks_combined_family_sync_states_and_components` proves that a single
  `TychoFeed` configured for `uniswap_v2 + uniswap_v3` can ingest a combined-family Tycho update,
  materialize components from both protocol systems into shared market data, retain per-protocol
  synchronizer readiness, and advance the shared `last_updated` marker from the latest ready
  branch
- Fynd-side user-facing quote flow now also has automatic replay proof under a combined-family
  universe:
  `test_combined_uniswap_recording_replays_user_facing_quote_path` proves a recorded market
  session whose metadata explicitly includes `uniswap_v2 + uniswap_v3` replays into a market that
  materializes both protocol systems, and that `Solver.quote()` still returns a successful,
  non-empty route whose swaps stay inside that combined protocol universe
- future-family extensibility is directly covered at the runtime-planning layer:
  `custom_registry_detects_future_family_without_runner_changes`
  proves a new family can be detected and planned without changing runner orchestration
- future-family extensibility is directly covered at the shared-bootstrap layer:
  `parses_future_family_params_through_custom_registry` and
  `builds_shared_bootstrap_plan_for_future_family_with_custom_registry`
  prove custom family registries can reuse shared bootstrap parsing and plan construction
- shared-bootstrap input hardening is now directly covered as well:
  `rejects_shared_bootstrap_plan_with_invalid_custom_registry` proves incomplete custom-family
  handler declarations fail immediately at plan construction, and
  `rejects_shared_bootstrap_plan_with_mismatched_inferred_families` proves protocol systems from
  different inferred families cannot be merged into one shared bootstrap plan even without
  explicit `family_runtime` declarations
- family-registry validation now also fronts the config surface itself:
  top-level `family_runtimes` defaults are rejected immediately when they name an unknown family,
  and registry-level shared-bootstrap eligibility checks reject future family defaults when not
  every declared member supports the shared bootstrap contract
- protocol-family source outputs now also preserve contract/account creation semantics for
  dynamically discovered pools:
  `pool_created_changes_include_pool_contract_address` in both the V2 and V3 substream crates
  proves new pool components carry their pool contract in `contracts`, and
  `protocol_changes_promote_created_pool_contracts_into_contract_changes` proves the V3 final
  protocol-changes path promotes those contracts into creation-style `contract_changes` so the
  shared runtime can persist the corresponding accounts before component-contract linking
- shared-bootstrap split semantics now also preserve contract-owned account changes:
  `splits_merged_family_bootstrap_block_by_protocol_system` proves a merged family bootstrap
  block can route both `account_deltas` and `account_balance_changes` back into the correct
  protocol branch using component-contract ownership, instead of rejecting those changes as an
  unsupported shared-bootstrap shape
- family dispatch now emits explicit empty branch blocks for untouched members:
  `dispatches_empty_branch_block_for_untouched_family_member` proves every shared-family block
  advances every member branch, even when only one branch carries component/state/storage changes;
  this closes the restart hole where some members persisted progress and others remained fresh

### Partially Proven / Still Inferred

- stable Fynd semantics are only partially evidenced:
  Tycho RPC semantics now have direct combined-family coverage; Fynd also has automatic proof for
  combined-family protocol wiring, feed-consumption/readiness semantics, and replayed
  user-facing quote success under an explicitly combined `uniswap_v2 + uniswap_v3` recording.
  Remaining uncertainty is now concentrated in the live end-to-end route-return and
  quote-settlement checks, which still remain ignored/manual tests against a local Tycho +
  live RPC environment rather than always-on repository proof
- combined-stream reorg behavior is only partially evidenced:
  reconnect and revert plumbing are covered in unit/integration tests, subscriber-level and
  persistence-level branch-failure isolation are directly covered, and there is now a DB-backed
  recovery regression that proves `Undo -> new canonical family blocks` across both V2 and V3
  branches; remaining uncertainty is mostly around even more production-shaped live chain paths,
  not around the core shared-runner reorg recovery contract itself
- true end-to-end factory discovery remains only partially evidenced:
  the repository now has direct DB-backed and restart-backed coverage for V2/V3 dynamic admission
  through the real combined-family handler semantics, but it still relies on repo-local synthetic
  block fixtures rather than a live combined package replay against historical chain data

### Not Yet Proven Enough To Close The Goal

- automatically exercised live combined-runtime Fynd E2E proof covering route return and
  quote settlement against a local Tycho + live RPC environment
- a live-history-style family regression that replays real combined-package output over a
  historical block slice, proving newly discovered pools join the indexed universe and continue
  receiving follow-up state updates under the shared family stream without relying on
  repo-local synthetic block construction

## Recommended Next Slice

The next implementation slice should be:

1. complete runtime validation for the optional combined entrypoint
2. add dynamic factory pool admission on top of the shared bootstrap seed model
3. add regression coverage that proves newly discovered V2/V3 pools become queryable through
   Tycho RPC and continue receiving follow-up updates

This keeps the bootstrap unification work intact while addressing the main remaining production
gap: factory-discovered pools must join the indexed universe automatically.

## Current Phase 3 Execution Plan

The current Phase 3 close-out sequence should be:

1. keep the shared family registry as the single source of truth for stream membership and shared
   bootstrap membership
2. continue moving family-scoped settings into shared config surfaces only where they are truly
   family-wide, avoiding new per-protocol drift
3. add a production-shaped regression where a real factory-created event from the combined
   Substreams package admits a new pool and carries follow-up state through storage and RPC
4. re-run combined-family restart/reconnect validation with that dynamic-admission path included
5. only then treat the Uniswap-family shared bootstrap + single-stream runtime as closed and use
   it as the template for the next protocol family
