# Uniswap V2/V3 Shared Bootstrap And Combined Substream Plan

## Status

- Phase 1: complete
- Phase 2: complete
- Phase 3: in progress

Current Phase 3 operator entrypoint:

- `scripts/check-combined-family.sh` is now the top-level validation surface
  - `command acceptance` / `run-acceptance` now mean the repo-local shared-runtime acceptance
    surface only
  - `command full` / `run-full` now mean repo acceptance plus live Fynd environment validation
  - `doctor` now also reports the canonical combined-family indexer operator readiness and
    startup command surface from `scripts/run-combined-family-indexer.sh`, while keeping
    `acceptance_ready` / `full_ready` scoped to the repo-local DB gate plus live Fynd gate
  - the top-level live step is no longer locked to the full two-test Fynd pass: setting
    `TYCHO_COMBINED_FAMILY_LIVE_SELECTION=route|settlement|all` narrows `command live`,
    `run-live`, `command full`, and `run-full` without changing the default `all` behavior
- `scripts/check-combined-family-db.sh` remains the repo-local DB-backed gate
- `scripts/check-combined-family-fynd-live-e2e.sh` remains the live Tycho/Fynd gate
  - the live gate no longer hard-codes the route and settlement ignored-test names internally:
    `FYND_E2E_ROUTE_TEST` and `FYND_E2E_SETTLEMENT_TEST` can override those selectors while the
    default contract still targets the current combined Uniswap-family tests
- `scripts/check-combined-family.sh` now also exposes optional managed live/full modes:
  `run-live-managed` and `run-full-managed` can start the canonical combined-family indexer
  entrypoint, wait for `/v1/health`, run the live Fynd E2E contract, and then tear the managed
  indexer process down again

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
- that inheritance surface is now slightly narrower as well: member and top-level family configs
  still need to resolve the shared `.spkg`, but `shared_module` can now be omitted when the family
  registry already declares the canonical output module, so adding a new config entrypoint for an
  existing family no longer needs to restate the shared module name just to reach the generic
  single-stream runtime path
- the built-in Uniswap family now consumes those defaults directly too: the checked-in combined
  extractor YAML no longer repeats `shared_module`, and the built-in family registry no longer
  has to restate canonical route aliases when `protocol_system` already normalizes to the desired
  route key
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
  `build_all_extractors(...)` can successfully build one shared family runner from configs that
  omit member-level `spkg`, `stop_block`, `bootstrap`, and member `substreams_params`, inheriting
  those values from top-level `family_runtimes.uniswap` defaults instead; that runtime coverage
  now includes both a pure family-default path and a seeded-progress path that proves the real
  combined entrypoint accepts top-level shared bootstrap defaults and member-scoped shared
  substreams params without falling back to per-extractor duplication
- the family runner now resolves and loads the runtime package from the detected family-level
  `shared_spkg`, instead of implicitly reusing the first member extractor's package path
- extractor configs can now declare `protocol_system` explicitly, and family-runtime resolution
  no longer depends on extractor config keys or `name` matching the protocol identity exactly
- shared route filtering now keys off `protocol_system` for family-enabled extractors, so aliased
  extractor ids do not break protocol-specific bootstrap or substreams pool selection
- that `protocol_system`-first routing rule now applies even outside the shared-family runtime:
  if a config explicitly declares `protocol_system`, bootstrap and substreams route filtering use
  it regardless of the extractor's local `name`, which removes another residual
  `extractor name == protocol_system` assumption from the config-loading path
- the shared-family bootstrap application path now follows that same rule too:
  extractors expose their stable `protocol_system` directly, and family bootstrap branch
  resolution no longer depends on the outer extractor-map key matching that protocol system,
  which means alias-named family members can still be found through the shared bootstrap path
  without reintroducing the old key-shape coupling
- the shared-family subscription path now follows it too:
  family handles may still carry alias-shaped extractor ids, but the runner resolves those
  handle names back onto the protocol-system keyed branch registry before attaching subscribers,
  so combined-stream branch delivery no longer depends on `handle.name == protocol_system`
- that branch-subscription lookup is now partially cached at the runner boundary too:
  protocol-system keyed branches are indexed eagerly, while alias-shaped handle names are learned
  on first subscription and memoized back into the family runner, so future family protocols do
  not need to reintroduce per-subscription branch scans just to preserve aliased handle support
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
- the remaining shared-stream metadata fallback rules are now narrower too:
  `shared_spkg`, `shared_module`, and `durability_scope` each have one explicit helper-level
  resolution surface instead of being re-derived ad hoc across config loading and family-runtime
  detection, which reduces the chance that future family integrations reintroduce divergent
  fallback behavior for the shared stream target
- those shared-stream fields are now also consumable as one resolved target shape rather than as
  independent strings, so family detection and runner/config helper surfaces can talk about one
  shared stream identity instead of reassembling `{spkg,module}` pairs at each use site
- top-level `family_runtimes.<family>` defaults now converge on the same idea too: config loading
  resolves member `family_runtime` inheritance through one family-default entrypoint instead of
  peeling `shared_spkg/shared_module/durability_scope` apart at the merge call site, which keeps
  the config/runtime contract narrower for future families
- that family-level config surface now composes across recursive extractor-config includes as
  well: duplicate `family_runtimes.<family>` entries are merged field-by-field instead of being
  rejected wholesale, so shared stream/bootstrap defaults and member-scoped param defaults can be
  layered through reusable fragments without collapsing back to one monolithic config file
- the family registry now also exposes family-level output-module lookup, so generic tooling and
  tests do not need to hard-code the current Uniswap merged module name just to build or validate
  a shared-family Substreams request
- that shared-stream metadata surface is less fragmented now too:
  family-level `output_module`, `shared_stream_name`, and `durability_scope` can now be consumed
  together through one registry metadata object, and shared-stream identity plus family-runtime
  default resolution build on that object instead of open-coding those fields separately at each
  callsite
- that same surface is now reachable from member protocol systems too:
  registry callers that start from `protocol_system` can resolve the shared-family runtime
  metadata directly instead of first resolving `family_name` and then re-querying per-field
  family metadata
- that registry-owned module metadata is now starting to flow into the recorder/tooling path too:
  `record-substreams` fixture helpers and Substreams-recording tests can resolve the merged family
  module through the registry instead of spelling the current Uniswap module literal at each call
  site
- `record-substreams` request shaping is narrower now too:
  resolved runtime targets can apply `start/stop/params` overrides through one shared
  Substreams-request override entrypoint, so the recorder path no longer open-codes a second
  copy of "default request plus CLI overrides" assembly outside the runtime-target layer
- runtime-target resolution is now starting to converge at the config boundary too:
  `ExtractorConfigs` can now project directly into resolved runtime targets, so the indexer
  entrypoint and `record-substreams` no longer rebuild family planning by hand from raw
  extractor maps
- that convergence now also covers runtime-target metadata projection:
  protocol-system and DCI-protocol views derived from resolved runtime targets now live in
  `family_runtime.rs` instead of staying as local `config.rs` helpers, so future entrypoints
  reuse one shared family-aware projection surface when they need runtime-target metadata beyond
- standalone startup now also has one shared progress snapshot surface:
  `load_extractor_progress_snapshot(...)` centralizes the persisted
  `cursor/last_processed_block/completed_bootstrap_block` shape used while resolving
  single-extractor startup state, so the next shared-runtime convergence work does not need to
  keep re-deriving that startup-progress bundle at each standalone call site
- runtime-target managed startup now converges one layer higher too:
  `ResolvedRuntimeTarget` first prepares a unified `PreparedManagedRunnerStartup` and only then
  fans back out into single-runner vs family-runner wiring, so the orchestration surface that
  turns runtime targets into managed runners no longer duplicates per-target startup sequencing
  before the final runner-specific assembly step
- shared bootstrap commit semantics now converge as well:
  once a bootstrap block has been materialized, both standalone and family paths now delegate the
  actual `handle_block_changes + flush + mark_bootstrap_completed` sequence into
  `commit_materialized_bootstrap(...)`, leaving only plan materialization and family-specific
  branch resolution outside the shared bootstrap commit pipeline
  the raw target list itself
- standalone RPC startup and full indexer startup now also share one config-owned launch surface:
  `ResolvedServiceLaunchConfig` owns `AUTH_API_KEY` / `plans.yaml` loading plus server
  bind-prefix-port state, and both `run_rpc(...)` and `create_indexing_tasks(...)` start Tycho
  services through that same launch object instead of open-coding separate service bootstrap
  paths in `main.rs`
- runtime-target execution metadata is also a bit more complete now:
  initialized-account preload requests are derived from `ResolvedRuntimeTarget` itself rather
  than being reassembled in `main.rs`, which narrows the remaining family-aware startup logic
  still owned by the binary entrypoint
- that preload surface is now narrower operationally as well: shared-family startup coalesces
  initialized-account requests by `(chain, block)` at the resolved runtime-target layer and
  de-duplicates repeated addresses before `main.rs` executes the preload, so adding more family
  members no longer implies repeating the same account bootstrap fetches just because multiple
  member configs requested the same seed contracts
- startup preload now also de-duplicates across runtime targets, not just within one family
  target: if a shared family and standalone extractors request the same initialized accounts at
  the same `(chain, block)`, `build_all_extractors(...)` issues one coalesced preload request
  before runner construction instead of replaying overlapping account snapshots target by target
- the binary entrypoint owns less of that preload implementation too: initialized-account
  extraction/writes now live under `extractor::startup`, while `main.rs` only invokes the
  resolved runtime-target preload helper, which reduces another piece of startup-specific state
- family-level member route-filter defaults are now part of that shared config surface too:
  `family_runtimes.<family>.members.<protocol_system>.shared_route_protocols` can override the
  registry-provided route alias set used for both shared bootstrap pool selection and shared
  substreams param filtering, so future families can adjust branch-local routing semantics
  through one top-level family config entrypoint instead of reintroducing per-extractor drift
- those route-filter defaults are now validation-backed at the YAML boundary as well: conflicting
  canonicalized aliases across family members fail config load, and member-default blocks that
  name protocol systems outside the registered family are rejected before runtime planning, which
  keeps the shared family registry as the single source of truth for both membership and
  route-filter ownership
  orchestration that future protocol families would otherwise inherit through the binary layer;
  `build_all_extractors(...)` no longer stitches together runtime-target account preload requests
  itself
- the shared-bootstrap completion proof now reaches the actual family stream request boundary too:
  a runner regression verifies that when the shared bootstrap completion marker already exists for
  every family branch, the combined Uniswap family opens one fresh Substreams request at
  `bootstrap_block + 1` with no resume cursor, rather than re-materializing bootstrap work or
  restarting the shared stream from the bootstrap block itself
- shared-family execution metadata is now carried as one resolved shared-stream object, instead of
  four parallel strings for `spkg`, `module`, `extractor_id`, and `durability_scope`; this makes
  the generic runtime path closer to “one family stream target in, one family stream runtime out”
  for future protocol-family integrations
- shared-family test fixtures are converging on the same registry contract too: the reusable
  `testing.rs` family block-response helper now resolves the merged output module by family name
  through the runtime registry, so future family-level tests do not need to hard-code the current
  Uniswap merged module literal just to fabricate Substreams responses
- additional tooling-adjacent tests now follow that same rule: `substreams/stream.rs` reconnect
  coverage and `cli.rs` record-substreams parsing resolve the Uniswap merged module and shared
  stream identity through the family runtime registry instead of asserting against baked-in
  literals
- the recorder/live-capture tooling path is now registry-extensible too: the internal
  `record-substreams` request derivation path accepts an injected family runtime registry, and
  `resolve_record_substreams_request_with_registry_derives_future_family_request` proves a custom
  future family can load family defaults from YAML, derive the shared stream request, preserve
  member-specific merged params, and compute the bootstrap-adjusted shared stream start/stop
  blocks without any Uniswap-specific request-building code
- that recorder request derivation now also lives outside the binary entrypoint itself:
  `main.rs` delegates family/standalone target selection, derived request resolution, and
  merged-params override handling to a dedicated `record_substreams` module instead of keeping
  another copy of family-aware runtime-target orchestration inline with CLI startup code
- the recorder execution path now follows that same split too:
  the `record-substreams` module owns request printing, package loading, Substreams record
  execution, and fixture writing, while `main.rs` only dispatches the CLI command into that
  module-level entrypoint
- the `main.rs` DB-backed family integration tests are starting to collapse the same local
  duplication too: repeated family block-response helpers now delegate into the shared
  `testing.rs` family fixture path instead of each test block re-encoding the merged Uniswap
  output-module envelope by hand
- that cleanup now also covers several distinct follow-up/restart/recovery test blocks under
  `main.rs`, which means the DB-shaped family integration surface is beginning to share one
  registry-backed fixture path instead of keeping separate local copies of the Uniswap family
  Substreams response wrapper
- some of the repeated family-runtime config scaffolding in `main.rs` is now starting to collapse
  as well: a local `uniswap_family_runtime(...)` helper centralizes the registry-derived shared
  module for DB/integration test configs, so those tests no longer need to restate the same
  `FamilyRuntimeConfig { family, shared_spkg, shared_module }` shape by hand each time
- that same cleanup now extends to the remaining inline family-default YAML test templates in
  `main.rs`: they inject the shared module name from the helper/registry path instead of baking
  `map_uniswap_family_protocol_changes` directly into ad hoc config strings, which removes another
  future-family rename hazard from the DB-backed integration surface
- shared bootstrap execution has now converged one step further as well: resolved family execution
  config carries one plan-level materializer function instead of a separate optional
  `shared_bootstrap_runtime` plus fallback logic inside the runner, and the registry now defaults
  that materializer to the generic branch-runtime merge path when a future family does not provide
  a family-level wrapper at all; `custom_registry_defaults_shared_bootstrap_plan_materializer_from_branch_runtimes`
  proves a custom family can rely on member branch materializers alone and still execute through
  the shared bootstrap planning surface
- that bootstrap execution surface is narrower internally now too: resolved family execution
  carries one `shared_bootstrap_execution` object rather than parallel
  `shared_bootstrap_plan_materializer` / `shared_bootstrap_branches` fields, so family bootstrap
  lifecycle code consumes one family-scoped execution decision instead of re-threading those
  coupled values independently
- the same execution object now reaches the standalone/member bootstrap path too: the
  single-extractor bootstrap runner resolves `ResolvedSharedBootstrapExecution` from the family
  registry via `protocol_system` and uses that object to materialize the bootstrap plan, so
  bootstrap orchestration no longer splits between a family-only resolved path and a separate
  default-registry free-function path
- the residual compatibility wrappers for that old path are gone as well: `shared_bootstrap.rs`
  no longer exports separate default-registry `materialize_*_block(...)` entrypoints, and the
  registry-owned `materialize_shared_bootstrap_plan(...)` path now reuses the same resolved
  execution object instead of reassembling plan-materializer and branch-runtime state a second
  time
- startup orchestration is narrower now too: `ResolvedRuntimeTarget` first resolves one target-
  owned prepared Substreams request shape and then loads the stream through one shared startup
  conversion path, while family startup now carries its `family_execution` metadata inside the
  prepared startup object instead of threading that state beside the startup payload as a
  separate parallel argument
- family subscription routing is slightly less ad hoc now too: alias-name and protocol-system
  branch lookup is owned by one `FamilyBranchSubscriptionIndex` helper instead of being
  reassembled across both family-runner construction and the live subscription path
- family branch runtime wiring is less runner-local now too:
  protocol-system keyed branch subscription-map creation plus per-branch handle assembly now live
  in `family_runner_wiring.rs` instead of staying embedded inside `runner.rs`, which keeps the
  family-specific wiring contract closer to the family runner/runtime boundary and narrows the
  amount of family-only orchestration left in the generic runner module
- family runtime execution is narrower now too:
  the shared-family new-block dispatch chain (`dispatch -> sort -> branch process -> propagate`)
  plus live branch-subscription attachment now live in `family_runtime_execution.rs`, leaving
  `runner.rs` with the outer control-loop skeleton while family-specific execution details move
  beside the other family runtime modules
- that same control-loop ownership has narrowed another step as well:
  family-specific control-message handling (`Stop` vs branch subscription) and
  `BlockResponse` handling (`New` / `Undo` / `Ended`) now resolve through
  `family_runtime_execution.rs`, so `runner.rs` no longer owns the family-only dispatch branches
  inside its `tokio::select!` loop and is closer to being just a generic loop shell around the
  shared-family runtime
- the remaining family loop-local state has narrowed with it too:
  the shared-family loop now carries a dedicated `FamilyRuntimeLoopState` plus one
  `handle_family_stream_item(...)` entrypoint in `family_runtime_execution.rs`, so family stream
  identity, partial-block tracking, and `Option<Result<BlockResponse, ...>>` handling no longer
  need to be open-coded in `runner.rs`
- family runner ownership is narrower at the struct boundary too:
  `FamilyExtractorRunner` no longer stores its dispatcher, protocol cache, subscription-index,
  or subscriber-counter fields directly; those family-only runtime concerns now live under one
  `FamilyRuntimeState`, while the runner constructor preserves the old external shape and builds
  that state internally so tests and startup wiring do not need to duplicate the assembly logic
- family runner definition ownership is narrower as well:
  the `FamilyExtractorRunner` type plus its `run()` implementation and test-only subscription
  helpers now live in `family_runtime_execution.rs`, while `runner.rs` only re-exports the type
  for the generic managed-runner surface; that removes another family-specific runner artifact
  from the generic runner module without changing the external builder/startup contract
- shared bootstrap plan construction is narrower for the same reason: the family runtime registry
  now owns the combined `family_name + branch descriptors + bootstrap_block` build path through
  one `build_shared_bootstrap_plan(...)` entrypoint, and both `shared_bootstrap.rs` and the
  resolved-family execution planner delegate to that registry-owned constructor instead of
  reconstructing the same family-scoped plan shape at multiple callsites
- family-runner execution metadata now also has one source of truth inside the runner layer:
  `build_family_runner(...)`, shared-bootstrap startup, and family stream-start resolution now
  consume `ResolvedFamilyExecutionConfig` directly instead of copying those fields into a second
  `FamilyRunnerContext` wrapper, which reduces drift risk between family-runtime planning and
  family-runner execution as additional protocol families are added
- runner-side extractor construction is now one step more converged too:
  both standalone runtime targets and shared-family runtime targets build branch extractors
  through the same `build_extractors_for_configs(...)` helper, so database batch sizing,
  partial-block handling, and base extractor-builder assembly no longer live in duplicated
  orchestration paths
- the runner-side Substreams start boundary is now converging on the same runtime-target
  contract too: both standalone extractors and shared-family runners materialize their live
  stream from one resolved Substreams execution-request shape, so future protocol families do
  not need a separate family-only field-plumbing path just to start the shared stream after
  bootstrap/resume state has been resolved
- that convergence now reaches the pre-stream execution step as well:
  family and standalone startup both materialize one `PreparedSubstreamsRequest { request,
  cursor }` shape before opening the stream, and the runner uses one shared
  `prepared request -> SubstreamsStream` helper instead of keeping separate family-only stream
  assembly at the last startup boundary
- standalone runner startup now follows the same staged shell as the family path too:
  `ExtractorBuilder` no longer jumps straight from “built extractor” to `into_runner()` only;
  it now exposes a prepared-startup stage plus shared runner/handle wiring, so standalone and
  family startup both pass through “prepare startup, then build managed runner” shells instead of
  reserving that two-stage orchestration model for the family runtime alone
- that startup-shell convergence now also covers package loading and stream assembly:
  family and standalone prepared-startup paths both call the same
  `load_stream_for_prepared_request(...)` helper, so the final
  `prepared request -> load .spkg -> open SubstreamsStream` sequence no longer exists as two
  parallel implementations
- shared-family stream identity is now registry-owned as well:
  family runtime specs declare the shared stream name and durability scope explicitly, and
  detected/resolved family runtimes carry those values forward instead of reconstructing
  `*_family` and `family::*` identifiers from naming conventions inside the planner
- that registry-owned durability scope now also drives config/default resolution for family
  members, so future protocol families can override the shared cursor/bootstrap storage scope
  without being forced back through the legacy `family::<name>` convention during config load
- the runner/build path now enforces that contract as well: family-enabled extractors must reach
  runtime construction with a resolved family durability scope already attached, instead of
  silently rebuilding or omitting that scope inside the extractor builder
- the same explicit-resolution rule now also covers the shared stream target itself: family
  planning/runtime paths require `shared_spkg` and `shared_module` to already be resolved on the
  family config instead of silently falling back to member-local `spkg` or `module_name`
- shared bootstrap completion is now coordinated at family scope too: before a shared-family run
  decides to skip or execute bootstrap, it validates that every branch sees the same completed
  bootstrap block instead of consulting one marker branch and implicitly trusting the rest
- that completion contract is now proven at the runner decision layer too:
  `run_family_bootstrap_if_needed(...)` has direct regression coverage showing that fully
  completed shared bootstrap state skips materialization entirely, while misaligned completed
  bootstrap blocks fail before any shared bootstrap execution begins
- bootstrap completion policy is now explicit across both startup modes:
  standalone startup and shared-family startup both call the same
  `decide_bootstrap_completion(...)` helper, but standalone extractors are allowed to rerun
  bootstrap when the configured block drifts while family startup still requires an exact
  configured-vs-persisted block match before it can skip or continue the shared bootstrap path
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
- shared-family process restarts now also resume the upstream stream from one validated shared
  cursor instead of only from `last_committed_block + 1`: family startup derives the persisted
  cursor from branch state, rejects branch-cursor drift early, and reuses that cursor for the
  single shared Substreams request
- that shared resume boundary is now narrower inside the runner too: the family runner resolves
  one unified shared stream position object from branch progress, deriving `start_block` and
  `cursor` together instead of validating them through separate helper passes, which reduces the
  chance that future family runtimes drift on restart/resume semantics inside the shared-stream
  orchestration layer
- shared bootstrap completion persistence is now less protocol-shaped too: once branch-local
  bootstrap blocks have been applied, the family runner writes the durable shared completion
  marker through any resolved family extractor instead of tying that write to the first branch in
  the bootstrap plan, which better matches the fact that combined-family bootstrap completion
  already lives under one shared family durability scope
- the extractor storage gateway now matches that contract more directly as well: shared-family
  cursor and bootstrap persistence flow through one shared state-scope setting instead of two
  parallel `cursor scope` / `bootstrap scope` inputs, reducing another place where future family
  integrations could accidentally configure only half of the shared durability boundary
- that shared durability contract now also reaches the gateway's internal naming/fallback logic:
  cursor and bootstrap state keys are derived through one state-kind helper surface, and shared
  scope lookup falls back to the legacy extractor-local state names through the same path instead
  of maintaining separate cursor/bootstrap naming branches inside `ExtractorPgGateway`

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
- shared-family durable progress is now converging as well: combined Uniswap branches persist
  bootstrap completion and extraction cursor state under one family-scoped durability key, while
  still falling back to legacy per-extractor keys during migration/restart
- family runtime detection and resolution now live behind explicit family-level interfaces in
  `family_runtime.rs`
- the top-level config loading path is now registry-parameterized too, not just the lower-level
  runtime/bootstrap helpers:
  `ExtractorConfigs::from_yaml` still uses the built-in registry by default, but the shared
  bootstrap merge path, shared substreams param resolution path, family-runtime defaulting path,
  and final resolved-runtime validation path now all accept an injected `FamilyRuntimeRegistry`;
  `custom_registry_loads_future_family_from_yaml_entrypoint` proves a future family can enter
  through the YAML config entrypoint, inherit top-level `family_runtimes` defaults, resolve
  shared bootstrap/shared substreams params, and survive final combined-runtime validation
  without any runner-specific code changes

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

Recent validation work tightened the first item further:

- shared family resume now reuses one validated shared cursor instead of depending only on
  `last_committed_block + 1`
- restart regressions now explicitly cover follow-up state application after dynamic component
  admission for both V2 and V3 branches
- serial DB regression helpers now auto-run migrations so shared-family restart/resume tests do
  not depend on a pre-migrated local database
- the strict shared-family DB gate now passes end-to-end through
  `scripts/check-combined-family-db.sh run`, which exercises the focused restart, reconnect,
  dynamic-admission, and fixture-backed history-slice regressions against an isolated Postgres
  database instead of only proving those paths through ad hoc one-off `cargo test` invocations

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
6. family-level durability identity is now explicit as well:
   - stream execution still has a dedicated stream extractor id such as
     `ethereum:uniswap_family`
   - durable shared state uses a separate family scope such as `family::uniswap`
   - future families no longer need runner-local string conventions to decide where shared
     cursor/bootstrap state should live

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

- repo-level combined extractor config builds exactly one Uniswap family runtime plan, resolves
  to exactly one shared family runtime target at the same boundary used by `tycho-indexer index`,
  and its checked-in V2/V3 bootstrap configs build one shared Uniswap bootstrap plan
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
- dispatcher-level routing now also covers the tighter same-block dynamic-admission shape:
  `routes_same_block_dynamic_admission_and_follow_up_updates`
  proves that a newly created family component can be admitted and receive entity, balance,
  contract, and storage follow-ups in the very same combined family block, without requiring a
  second block to establish component or contract ownership first
- the same seeded-universe contract is now directly covered for V3 as well:
  `combined_family_runner_v3_dynamic_component_joins_seeded_universe_and_receives_follow_up_state`
  proves that a shared-bootstrap-seeded V3 universe is not treated as a permanent bootstrap
  allowlist, and that a later V3 `PoolCreated -> Swap` path can admit a new pool while keeping
  the pre-seeded V3 component visible in the same combined runtime
- shared-family restart semantics are now also covered after dynamic admission:
  `combined_family_runner_restart_applies_follow_up_state_after_dynamic_component_admission`
  and `combined_family_runner_restart_applies_v3_follow_up_state_after_dynamic_component_admission`
  prove that once a dynamically discovered V2 or V3 pool is admitted through the shared stream, a
  fresh process restart resumes the shared family at the next block, applies the later follow-up
  state under the shared runtime, and does not fall back into mixed fresh-vs-resumed branch
  progress
- external Tycho API semantics have direct combined-family evidence at the component/state level:
  the same DB-backed regression verifies `/v1/protocol_components` and `/v1/protocol_state`
  for a dynamically admitted pool under the shared runtime
- Fynd routing semantics now have a repo-local dynamic-admission replay proof as well:
  `fynd-core/tests/integration/solution_tests.rs::test_combined_uniswap_dynamic_admission_becomes_routable_in_replay`
  replays a seeded combined-family universe where only a weaker V2 two-hop path exists at first,
  then admits a later V3 direct pool through a subsequent `new_pairs` update, and finally proves
  the quoted route selects that dynamically admitted pool in the final replayed market state
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
- the shared runtime now also has one mixed-protocol real-history-slice regression:
  `combined_family_runner_replays_real_v2_and_v3_history_slice_in_one_shared_session`
  proves a single shared Substreams session can replay a real V2 `PairCreated -> Sync` slice and
  a real V3 `PoolCreated -> Swap` slice in sequence, with both dynamically admitted pools becoming
  queryable and retaining follow-up state under the same combined-family runtime, and now also
  asserts that those V2/V3 pools are visible through `/v1/protocol_components` and
  `/v1/protocol_state`, not just through direct gateway reads
- that mixed-protocol history-slice proof is now also anchored in a committed serialized
  Substreams fixture instead of only an in-test response builder:
  `combined_family_real_history_slice_fixture_matches_generated_script` now proves the checked-in
  `crates/tycho-indexer/tests/fixtures/combined_family_real_history_slice.json` remains a
  successful live-captured shared-family session rooted at the configured start block and covering
  follow-up live blocks beyond the synthetic in-repo smoke script, rather than incorrectly
  requiring the committed fixture to remain byte-for-byte aligned with the old six-response
  handwritten builder, and
  `combined_family_runner_replays_fixture_backed_v2_and_v3_history_slice_in_one_shared_session`
  proves the same DB-backed shared runner can consume that serialized fixture and preserve the V2
  and V3 dynamic component/state assertions under the single shared session, including the same
  `/v1/protocol_components` and `/v1/protocol_state` visibility checks
- the repository now also has an explicit path to replace repo-generated fixtures with captured
  real combined-package output:
  `tycho-indexer record-substreams` reuses the production Substreams request shape, can load the
  package from the same local-or-S3 resolution path as the runner, and writes responses directly
  into the existing `MockSubstreamsScript` fixture format used by the shared-family regressions;
  `records_scripted_substreams_responses_into_fixture_format` now proves that recorder output
  round-trips through the committed fixture serializer and remains consumable by the replay path;
  `record_substreams_fixture_writes_replayable_fixture_via_command_path` now also proves the
  command-parsing entrypoint can drive that same recorder flow end-to-end instead of only testing
  the lower-level `SubstreamsEndpoint::record` helper in isolation; that recorder entrypoint can
  now also derive one shared family request directly from a checked-in combined extractor config
  instead of requiring hand-copied `spkg/module/start-block/params`, and
  `record_substreams_fixture_derives_shared_family_request_from_combined_config` proves the
  command path resolves the family-level shared package/module, bootstrap-adjusted shared start
  block, merged member params, and one final shared request before serializing the captured
  fixture; `resolve_record_substreams_request_derives_shared_family_request_from_repo_combined_config`
  now also proves the repository's actual checked-in
  `extractors.uniswap_v2_v3.combined.yaml` still resolves the same shared Uniswap request shape
  expected by operators, including the checked-in family package path and protocol-scoped V2/V3
  params; and `repo_combined_uniswap_family_record_args_can_anchor_fixture_refresh_workflow`
  proves the repository now has one reusable recorder-spec entrypoint that can anchor future
  replacement of the committed `combined_family_real_history_slice.json` fixture without
  reassembling the combined-family CLI arguments by hand; and
  `combined_family_real_history_slice_capture_spec_anchors_live_fixture_refresh` now pins one
  canonical repository-side capture spec for that committed fixture path, block window, and
  shared-family recorder flow so the remaining gap is executing the live capture rather than
  rediscovering the spec; `combined_family_real_history_slice_capture_spec_builds_stable_repo_cli_args`
  now fixes the exact argv surface for that refresh path, and
  `combined_family_real_history_slice_capture_spec_renders_stable_shell_command` fixes the
  shell-rendered operator command that should be used for the live capture itself
- that recorder path now also has a no-network preflight mode:
  `record-substreams --print-request` resolves the effective shared-family request and prints the
  final `spkg/module/start/stop/params` JSON without opening a Substreams session, and
  `record_substreams_print_request_short_circuits_before_network_recording` plus
  `render_record_substreams_request_json_includes_resolved_combined_family_fields` prove the
  repository can validate the live capture request shape before spending a concurrent stream slot
- the repository now also exposes that preflight/capture workflow through one checked-in operator
  script:
  `scripts/combined-family-history-slice-fixture.sh preflight|command|record` fixes the same
  family config, fixture path, and history-slice block window at the repo boundary so live
  fixture refresh no longer requires manually reconstructing the combined recorder argv
- that script/spec boundary is now guarded against drift as well:
  `combined_family_real_history_slice_script_stays_aligned_with_capture_spec` proves the checked-in
  shell helper still targets the same start block, stop window, family/config selection, fixture
  path, and preflight mode expected by the code-owned recorder capture spec
- the final live-capture blocker was surfaced as data rather than process knowledge:
  `scripts/combined-family-history-slice-fixture.sh command` renders the exact recorder argv even
  when `SUBSTREAMS_API_TOKEN` or network endpoints are missing; after switching the Substreams
  client TLS roots from native keychain-backed roots to `webpki` roots, the repository-side live
  recorder now also runs successfully on this host without the earlier `No keychain is available`
  failure, so the repo no longer depends on that host-specific TLS state just to refresh the
  committed shared-family fixture
- that printed command now also has direct repository proof:
  `combined_family_real_history_slice_script_command_renders_stable_live_capture_command` executes
  the checked-in shell helper in `command` mode and proves it still renders the exact
  placeholder-backed live capture command expected by the code-owned capture spec
- the missing external prerequisites are now machine-checkable too:
  `scripts/combined-family-history-slice-fixture.sh doctor` reports whether the token and live
  endpoints are present for the final capture, and
  `combined_family_real_history_slice_script_doctor_reports_missing_external_requirements` proves
  the repository detects and renders the remaining blocker as `ready=false` plus explicit
  `missing` fields instead of leaving the final step implicit
- that readiness check can now also act as a hard gate:
  `scripts/combined-family-history-slice-fixture.sh doctor --strict` exits non-zero when the
  external token or endpoints are missing, and
  `combined_family_real_history_slice_script_doctor_strict_fails_when_env_is_incomplete` proves
  the final live-capture prerequisite check is now CI/automation-friendly instead of only
  human-readable
- shared-family restart semantics now also have the same real-creation-path coverage for V3 as
  for V2:
  `combined_family_runner_restart_applies_v3_follow_up_state_after_dynamic_component_admission`
  proves a pool admitted through the real V3 `PoolCreated -> family block` path survives a fresh
  process restart, resumes from the next shared-family block, persists a later follow-up
  state update under the shared runtime, and remains queryable through
  `/v1/protocol_components` and `/v1/protocol_state` after that restart
- shared-family restart coverage now also reaches the mixed V2+V3 history-slice path itself:
  `combined_family_runner_restart_replays_real_history_slice_from_persisted_cursor` splits the
  shared-session history-slice replay across two process lifetimes, proves the family cursor
  persists after the V2 follow-up and V3 creation blocks, resumes the shared stream at the V3
  follow-up block, and keeps both the earlier V2 pool and the resumed V3 pool queryable through
  Tycho RPC after restart; and
  `combined_family_runner_restart_replays_fixture_backed_real_history_slice_from_persisted_cursor`
  proves the same restart wiring can be driven from the committed recorded fixture by splitting
  the fixture responses into restart-sized chunks instead of rebuilding the replay only from
  repo-generated scripts
- hot reconnect behavior is now covered on top of that same dynamic-admission path as well:
  `combined_family_runner_reconnect_applies_v2_follow_up_state_after_dynamic_component_admission`
  proves a V2 pool admitted from a real family creation block survives an upstream gRPC failure
  inside the same process, that the shared runner reconnects with the emitted cursor, and that
  the newly admitted pool still receives and persists its follow-up state while remaining
  queryable through `/v1/protocol_components` and `/v1/protocol_state`; and
  `combined_family_runner_reconnect_applies_v3_follow_up_state_after_dynamic_component_admission`
  proves a V3 pool admitted from a real `PoolCreated` family block survives an upstream gRPC
  failure inside the same process, that the shared runner reconnects with the emitted cursor
  instead of replaying from the original start block, and that the newly admitted pool still
  receives and persists its follow-up state after reconnect while remaining queryable through
  `/v1/protocol_components` and `/v1/protocol_state`
- family-runner orchestration has now been tightened around one explicit family runtime context
  instead of re-deriving family-wide bootstrap/start settings at each runner step:
  `FamilyRunnerContext` now precomputes the shared bootstrap plan and the bootstrap-adjusted
  shared stream start block, `build_family_runner` validates those family-wide invariants before
  bootstrap/stream execution, and
  `test_family_runner_context_precomputes_shared_bootstrap_plan_and_start_block` plus
  `test_resolve_family_stream_start_uses_bootstrap_adjusted_aligned_fresh_start` prove the runner
  consumes that precomputed family context rather than re-scanning per-branch config at startup
- that family-wide execution planning is now starting to live with the family-runtime planner
  instead of only inside the runner:
  `resolve_resolved_family_execution_config(...)` in `family_runtime.rs` now resolves the
  shared-family branch-routing specs, merged params, shared stop block, bootstrap-adjusted
  shared start block, and shared bootstrap plan for an already-detected family, and
  `FamilyRunnerContext::from_resolved_family` now consumes that planning result instead of
  duplicating the same config-derived resolution in runner-local code
- family-runtime planning now also carries the shared-bootstrap execution hooks themselves, not
  just the bootstrap plan data:
  `ResolvedFamilyExecutionConfig` now preserves the family-level bootstrap materializer plus the
  per-member branch materializers, and `run_family_bootstrap_if_needed(...)` consumes that
  resolved execution state directly instead of re-discovering bootstrap execution through the
  global default registry at runner time
- the `record-substreams` derived-family path now reads from the same resolved execution object as
  the runtime path:
  `resolve_record_substreams_request(...)` no longer re-derives family `spkg`, `module`,
  bootstrap-adjusted `start_block`, `stop_block`, and merged substreams params by iterating member
  configs itself; it now consumes `ResolvedFamilyRuntime.execution`, which keeps fixture capture
  and live runtime startup aligned on one family-level source of truth
- shared request shaping now converges on that same runtime surface as well:
  `ResolvedRuntimeTarget::substreams_execution_request_with_start_block(...)` lets the runtime
  layer emit the fully shaped family or standalone request for an already resolved effective
  start block, so `build_family_runner(...)` no longer mutates `request.start_block` after
  request construction
- that recorder convergence is now also structurally unified at the selector layer:
  derived `record-substreams` request resolution no longer maintains separate family and
  standalone config-walk branches; both modes now select one `ResolvedRuntimeTarget` and then
  derive the effective Substreams request through the same target-level execution surface, which
  removes another future-family extension point from `main.rs`
- that selector behavior is now owned by the shared runtime library rather than the binary too:
  `ResolvedRuntimeTargetSelector`, `ResolvedRuntimeTarget::matches_selector(...)`, and
  `select_resolved_runtime_target(...)` now live in `family_runtime.rs`, so future family-aware
  CLI/debug entrypoints can reuse one library-level target-selection contract instead of
  re-implementing family-vs-standalone matching in each binary command path
- the indexer startup allowlists now also honor explicit protocol identity instead of local config
  keys:
  `GatewayBuilder` protocol-system registration, RPC service protocol allowlisting, and the DCI
  protocol list are now derived from `ExtractorConfig::protocol_system()` with de-duplication,
  so aliased extractor ids can no longer leak into externally visible protocol-system filtering or
  bootstrap/runtime registration paths
- that protocol-system view now belongs to the config layer rather than the binary entrypoint:
  `ExtractorConfigs::protocol_systems()` and `ExtractorConfigs::dci_protocol_systems()` now own
  the de-duplicated explicit-protocol projection, which keeps startup wiring in `main.rs` from
  re-implementing extractor-config semantics and gives future entrypoints one shared config-level
  source of truth for protocol registration
- that convergence is now exposed one level higher as well:
  `ResolvedRuntimeTarget::substreams_execution_request()` derives the concrete package/module/start/
  stop/params payload for both family and standalone targets, so future CLI, recorder, or debug
  entrypoints can reuse one target-level substreams execution API instead of re-encoding separate
  family-vs-standalone request assembly rules
- runtime startup in `main.rs` is now starting to follow the same shape:
  `build_all_extractors(...)` no longer open-codes the full family-vs-standalone runner assembly
  loop inline; it delegates through `build_runner_for_runtime_target(...)`, which keeps the target-
  dispatch boundary explicit and reduces one more place where future shared-runtime changes would
  otherwise need to be mirrored across separate startup branches
- that startup helper now lives in library code rather than only in the binary entrypoint:
  `build_runner_for_runtime_target(...)` has been moved into `extractor/runner.rs`, so target-
  level runner construction is no longer a `main.rs`-local implementation detail and future
  runtime/debug harnesses can reuse the same target-dispatch construction path
- that runner-side convergence now covers the batch path too:
  `build_all_extractors(...)` no longer owns the loop that turns resolved runtime targets into
  `(ManagedRunner, ExtractorHandle)` groups; `extractor/runner.rs` now exposes
  `build_runners_for_runtime_targets(...)`, so both per-target and multi-target runner assembly
  live behind the same extractor-layer surface instead of keeping one last batch-orchestration
  detail embedded in `main.rs`
- the shared-family lifecycle logic has now moved out of the runner body as well:
  bootstrap-completion checks, branch-progress consistency, shared bootstrap application, and
  shared stream-position resolution now live in `extractor/family_lifecycle.rs`, so the family
  runner consumes one helper surface for startup/resume semantics instead of keeping that
  state-machine inline beside transport setup
- the resume side of that lifecycle is narrower now too: `family_lifecycle.rs` resolves one
  explicit family-level resume state before deriving the shared stream `start_block` and cursor,
  so aligned branch progress, aligned shared cursor reuse, bootstrap-only marker handling, and
  resume-block overflow checks no longer live as partially duplicated local branches inside the
  final request-shaping path
- that convergence now reaches the runner test surface too:
  ad hoc `ResolvedFamilyExecutionConfig` construction helpers no longer live in `runner.rs`;
  test-only family execution assembly now comes from `family_runtime.rs`, so even regression
  scaffolding is starting to use the same runtime-planning layer instead of restating family
  execution shape locally inside runner tests
- the DB/integration mock-stream surface is starting to collapse the same way:
  common Session/Undo response wrappers now live in `testing.rs`, and several `main.rs`
  combined-family regressions reuse those helpers instead of defining local Substreams session
  envelopes inline for each revert/restart/reconnect case
- the same cleanup now covers one class of family-default config scaffolding too:
  repeated Uniswap family-default YAML writers for restart/reconnect-style DB regressions are now
  starting to route through one `testing.rs` helper instead of embedding the same top-level
  `family_runtimes.uniswap` template in each `main.rs` test block
- the remaining Uniswap family runtime test shape is converging the same way:
  `main.rs` no longer owns the local helpers that resolve the shared output module or assemble a
  `FamilyRuntimeConfig` for the Uniswap family; those test-only builders now live in `testing.rs`
  so family runtime fixture shape is less coupled to the DB integration entrypoint
- some of the lowest-value wrapper layers are now disappearing entirely:
  several `main.rs` dynamic-admission regressions call the shared family block-response helper
  directly instead of routing through one-off local `family_block_response(...)` functions that
  only fixed a cursor label and timestamp convention
- that specific wrapper class is now gone from `main.rs`:
  the remaining local `family_block_response(...)` helpers used by the combined-family
  revert/recovery DB regressions have been removed, leaving those tests to call the shared
  `testing.rs` helper directly with explicit cursor/timestamp inputs
- the same is now true for the block-changes variant:
  `main.rs` no longer defines local `family_block_response_from_block_changes(...)` wrappers for
  V3 restart/reconnect regressions; those tests now call the shared `testing.rs` helper directly
  instead of forwarding through another layer that only reversed argument order
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
  materializes both protocol systems, and exercises `Solver.quote()` through that combined
  protocol universe under the aligned replay fixture/config baseline
- Fynd-side HTTP quote surface now also has automatic replay-equivalence proof:
  `test_quote_endpoint_replays_combined_family_fixture` proves that a replay-built `AppState`
  serving `/v1/quote` preserves the direct combined-family router result for sampled replay
  requests, including status parity and route-presence/swap-count parity when a route exists
- future-family extensibility is directly covered at the runtime-planning layer:
  `custom_registry_detects_future_family_without_runner_changes`
  proves a new family can be detected and planned without changing runner orchestration
- future-family extensibility is now covered at the auxiliary decode path too:
  `test_handle_tick_scoped_data_routes_custom_family_events_through_injected_decoders`
  proves a branch extractor can consume auxiliary protocol messages from decoder definitions
  supplied by the active family runtime, instead of only relying on the built-in default-family
  registry
- future-family extensibility is directly covered at the shared-bootstrap layer:
  `parses_future_family_params_through_custom_registry` and
  `builds_shared_bootstrap_plan_for_future_family_with_custom_registry`
  prove custom family registries can reuse shared bootstrap parsing and plan construction
- future-family extensibility is now covered through the full recorder path too:
  `record_substreams_fixture_with_registry_records_future_family_request`
  proves `record-substreams` can resolve the shared-family request, merge member params, and
  write fixture output through an injected family registry without adding new hard-coded family
  branches in `main.rs`
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
  combined-family protocol wiring, feed-consumption/readiness semantics, replayed
  `Solver.quote()` behavior, and `/v1/quote` handler equivalence over an explicitly combined
  `uniswap_v2 + uniswap_v3` recording. Remaining uncertainty is now concentrated in the live
  end-to-end route-return and
  quote-settlement checks, which still remain ignored/manual tests against a local Tycho +
  live RPC environment rather than always-on repository proof
- combined-stream reorg behavior is only partially evidenced:
  reconnect and revert plumbing are covered in unit/integration tests, subscriber-level and
  persistence-level branch-failure isolation are directly covered, and there is now a DB-backed
  recovery regression that proves `Undo -> new canonical family blocks` across both V2 and V3
  branches; remaining uncertainty is mostly around even more production-shaped live chain paths,
  not around the core shared-runner reorg recovery contract itself
- end-to-end factory discovery is now proven at the repository boundary:
  the checked-in `combined_family_real_history_slice.json` fixture is now a real combined-package
  historical capture, `combined_family_real_history_slice_fixture_matches_generated_script`
  proves it is richer than the in-repo synthetic smoke script, and both
  `combined_family_runner_replays_fixture_backed_v2_and_v3_history_slice_in_one_shared_session`
  and `combined_family_runner_restart_replays_fixture_backed_real_history_slice_from_persisted_cursor`
  prove dynamically discovered V2/V3 pools join the indexed universe and keep receiving follow-up
  state under the shared family stream using that committed live-style replay input

### Not Yet Proven Enough To Close The Goal

- automatically exercised live combined-runtime Fynd E2E proof covering route return and
  quote settlement against a local Tycho + live RPC environment
- that remaining live check is now at least operationally standardized:
  `scripts/check-combined-family-fynd-live-e2e.sh` provides `doctor`, `command`,
  `run-route`, `run-settlement`, and `run-all` modes against the sibling `fynd` repository, so
  the only remaining gap is whether to promote that live operator workflow into a formal
  acceptance requirement rather than leaving it as a manual gate
- the combined-family Tycho startup path is now standardized too:
  `scripts/run-combined-family-indexer.sh` provides `doctor`, `command`, and `run` modes for the
  canonical `extractors.uniswap_v2_v3.combined.yaml` entrypoint, including the local
  `AUTH_API_KEY=dummy` default and the safe `export SUBSTREAMS_API_TOKEN=...` pattern that avoids
  the shell-expansion bug where `--api_token "$SUBSTREAMS_API_TOKEN"` becomes empty when the token
  is only assigned inline for that single command
- that startup entrypoint is also less name-locked now: `TYCHO_INDEXER_ENTRYPOINT_LABEL` can
  override the human-facing label reported by `doctor` and usage text, while the default contract
  remains the canonical combined-family startup path and config
- that startup/operator surface is now regression-covered inside the repo as well:
  `main.rs` locks the script's `doctor`, `command`, strict-failure, and usage/contract shape so
  the canonical combined-family entrypoint cannot silently drift away from the documented
  workflow
- dynamic family admission is slightly less duplicated internally too: the family dispatcher now
  resolves `component_change -> protocol_system` ownership and registers both component and
  contract mappings through one admission helper instead of repeating that same ownership update
  flow across separate pre-registration and transaction-splitting paths

## Recommended Next Slice

The next implementation slice should be:

1. keep the manifest-backed DB gate green as the shared-family runtime continues to converge
2. keep the checked-in live-history fixture aligned with the combined config and refresh it when
   the combined package or seed universe changes materially
3. decide whether live Fynd E2E should become an explicit operational gate or remain a manual
   environment validation outside the repository acceptance surface

## Current Phase 3 Execution Plan

The remaining Phase 3 close-out sequence should be:

1. keep the shared family registry as the single source of truth for stream membership and shared
   bootstrap membership
2. continue moving family-scoped settings into shared config surfaces only where they are truly
   family-wide, avoiding new per-protocol drift
3. keep the refreshed real-capture combined-family history-slice fixture current as shared-family
   config or package shape evolves
4. keep the DB-backed combined-family restart/reconnect/history-slice regressions green as the
   runtime converges further, now that the checked-in strict gate passes in a local Postgres-backed
   environment
5. decide whether live Fynd E2E remains a manual/operator validation or is promoted into a formal
   acceptance gate, now that the top-level shared gate can optionally manage indexer startup rather
   than depending on a separately launched healthy Tycho instance
6. only then treat the Uniswap-family shared bootstrap + single-stream runtime as closed and use
   it as the template for the next protocol family

The DB-backed gate is now explicit at the shared test harness boundary too:

- serial Postgres test helpers still skip locally by default when `DATABASE_URL` is unreachable,
  but that behavior is now regression-tested in `tycho-storage`
- setting `TYCHO_REQUIRE_TEST_DB=1` or running under `CI` upgrades those same DB-backed tests
  from "skip with explanation" to a hard failure when the database is unavailable, so restart /
  reconnect / history-slice validation can be made non-optional in the environments that are
  supposed to prove Phase 3 close-out
- `scripts/check-combined-family-db.sh` now packages the focused Phase 3 DB-backed close-out
  gate as one repo-local entrypoint: `doctor` reports whether `DATABASE_URL` is reachable,
  `list` prints the exact restart/reconnect/history-slice test set, and `run` executes those
  tests with `TYCHO_REQUIRE_TEST_DB=1` so the shared-family runtime can be validated without
  rediscovering the correct serial-db subset by hand
- that same DB gate now diagnoses the most common local readiness failure directly too:
  `doctor` reports Docker CLI / daemon availability plus the exact `TYCHO_IMAGE=alpine docker
  compose ... up -d db` command needed to start the local Postgres dependency, and `run` now
  fails at that strict preflight boundary before entering `cargo test` when the DB is not ready
- the gate still stays environment-agnostic though: any reachable Postgres can satisfy it via
  `DATABASE_URL`, so the Docker compose path is documented and surfaced as the default local
  bootstrap path rather than as a hard runtime dependency of the Phase 3 validation surface
- that DB gate is now source-anchored too: the focused test list lives in
  `crates/tycho-indexer/tests/combined_family_db_gate.tests`, the shell entrypoint reads that
  manifest instead of hard-coding names, and `main.rs` regression coverage verifies both that the
  manifest entries still correspond to real shared-family tests and that the shell script's
  `list` output stays aligned with the same manifest
- that shell-entrypoint contract coverage is now broader too: `main.rs` regressions also lock
  the `doctor` diagnostic surface, the rendered `db-command` bootstrap command, the rendered
  `command` loop over the manifest-backed strict DB gate, and the `run` mode's fail-fast
  preflight behavior on an unreachable `DATABASE_URL`, so the remaining Phase 3 proof gap is now
  the external DB-backed execution itself rather than ambiguity about what the local validation
  entrypoint would do
- that external DB-backed execution gap has now also been re-verified in a real local Postgres
  environment through `scripts/check-combined-family-db.sh run`: the checked-in strict manifest
  completed end-to-end for the six fixture-backed history-slice / restart / reconnect regressions,
  so the remaining Phase 3 gap is no longer whether the repo-local DB gate runs successfully, but
  whether additional live-environment promotion is warranted beyond that repo-backed proof surface
- the DB gate manifest itself is now constrained to keep the strongest shared-session external
  semantics proof in scope: `main.rs` verifies that
  `test_serial_db::combined_family_runner_replays_fixture_backed_v2_and_v3_history_slice_in_one_shared_session`
  remains in `combined_family_db_gate.tests`, so the close-out gate cannot silently drop the
  fixture-backed `/v1/protocol_components` and `/v1/protocol_state` validation while still
  passing the looser "all referenced tests exist" check
- the checked-in history-slice recorder spec now derives even less Uniswap-specific metadata by
  hand too: repo-combined recorder tests resolve the unique family runtime target from
  `extractors.uniswap_v2_v3.combined.yaml` and reuse that resolved family name / output module /
  shared `.spkg` identity instead of duplicating those strings inside the capture-spec helpers,
  which reduces another place where future package-version or family-identity changes could drift
  away from the config the runtime actually consumes
- the recorder entrypoint itself now follows that same direction too: when
  `record-substreams --extractors-config ...` resolves exactly one runtime target, the command can
  derive the family or standalone target automatically instead of forcing a redundant
  `--family` / `--protocol-system` selector; the checked-in combined-family history-slice helper
  now defaults to that auto-resolution path and only injects `TYCHO_COMBINED_FIXTURE_FAMILY` when
  an explicit override is needed
- family-dispatch startup wiring is a bit thinner now too: protocol-cache preseed construction for
  the shared-family dispatcher lives on `FamilyBlockChangesDispatcher::from_protocol_cache(...)`
  instead of as a runner-local helper, so runner build now consumes the family-dispatch module's
  own preload entrypoint rather than reconstructing dispatcher seed logic at the orchestration
  layer
- protocol-level auxiliary message decoding is less hard-coded too: the
  `ProtocolExtractor` tick-scoped message loop no longer embeds an inline
  `protocol_system == "uniswap_v3"` branch for `Events` payloads and instead routes
  non-`BlockChanges` payloads through `protocol_message_registry.rs`; the only registered decoder
  is still Uniswap V3 today, but future protocols can extend that registry without pushing new
  protocol-name conditionals back into the core extractor loop
- that decoder surface is narrower now as well: the concrete Uniswap V3 auxiliary-message
  decoder and its type-url registration live in `family_uniswap.rs`, while
  `protocol_message_registry.rs` only aggregates family-provided decoder groups and performs the
  generic lookup; adding another family-level auxiliary message no longer requires teaching the
  core registry module how to decode that family's protobuf payloads directly
- the built-in registration path is narrower for the same reason: `protocol_message_registry.rs`
  no longer imports Uniswap-family decoder groups directly and instead resolves them through
  `family_registry.rs`, so built-in family runtime registration and built-in auxiliary-decoder
  registration no longer drift through parallel hard-coded entrypoints
- that convergence is one step tighter now as well: built-in auxiliary decoder groups are carried
  directly on `FamilyRuntimeSpec`, and `protocol_message_registry.rs` now walks
  `default_family_runtime_specs()` instead of consulting a second registry-specific export, which
  means the built-in family runtime spec itself is the single registration surface for both
  shared-runtime metadata and auxiliary protocol-message decoding
- that auxiliary-decoder abstraction is now exercised through the real extractor path too:
  `test_handle_tick_scoped_data_routes_uniswap_v3_events_through_registry` drives a
  `BlockScopedData` payload with the canonical Uniswap V3 `Events` type URL through
  `handle_tick_scoped_data(...)`, proving the registry-backed dispatch is not just a unit helper
  around direct decoder calls
- shared bootstrap completion scope is now proven more directly as well: a serial DB regression in
  `protocol_extractor.rs` persists the bootstrap marker through one shared-scope family branch
  gateway and then reads it back through a different family branch gateway, which confirms the
  durable bootstrap checkpoint is actually family-scoped storage state rather than a
  single-extractor implementation detail
- family branch runtime wiring is thinner for the same reason: branch-scoped subscription-map and
  handle creation now go through the family-runner-owned `FamilyBranchRuntimeWiring` assembly
  path instead of being open-coded inside `build_family_runner(...)`, which keeps protocol-system
  keyed branch wiring beside the family runner type that owns it
- that branch wiring has now converged one step further too: startup-time branch extractor,
  subscription-map, and handle assembly now flow through one `FamilyBranchRuntimeWiring`
  structure, while `FamilyExtractorRunner::new(...)` derives branch subscription routing directly
  from the protocol-system keyed extractor map instead of rebuilding a second subscription-key map
  from a parallel subscriptions shape
- family startup request shaping is thinner now too: the bootstrap-if-needed plus
  stream-position/cursor resolution path now goes through
  `prepare_family_substreams_request(...)` in `family_lifecycle.rs`, so
  `build_family_runner(...)` consumes one family-owned lifecycle entrypoint from shared bootstrap
  through shared Substreams request assembly instead of open-coding that startup sequence in the
  runner
- family startup assembly is narrower for the same reason: branch-extractor materialization now
  collapses onto protocol-system keyed startup artifacts through
  `FamilyExtractorRunner::prepare_startup(...)` and
  `FamilyExtractorRunner::extractors_by_protocol_system(...)`, so `build_family_runner(...)` no
  longer owns the intermediate "builders -> keyed extractors -> prepared stream" conversion path
- the family build entrypoint is now owned by the family runner as well:
  `build_runner_for_runtime_target(...)` delegates the complete shared-family construction path to
  `FamilyExtractorRunner::build_managed(...)`, and the remaining final assembly step also lives on
  `FamilyExtractorRunner::build_managed_from_startup(...)` instead of being split across a
  free-standing family builder function plus runner-owned internals
- the runtime-target dispatch surface is narrower too: managed-runner construction now hangs off
  `ResolvedRuntimeTarget::build_managed_runner(...)`, so the generic runtime-target entrypoint no
  longer owns the standalone-vs-family branch itself and instead delegates both paths through the
  resolved runtime target abstraction that already owns shared stream metadata and startup account
  derivation
- runtime-target collection semantics are starting to converge for the same reason:
  `ResolvedRuntimeTargets` now owns protocol-system summarization, DCI summarization, coalesced
  initialized-account requests, and selector/unique-target resolution as one wrapper over the
  resolved runtime-target set, and the `config.rs` / `record-substreams.rs` entrypoints now
  consume that wrapper instead of rebuilding those list-level semantics through separate free
  helper calls
- the shared-bootstrap seed validation path now follows that same direction too:
  repo bootstrap-seed extraction and seed-universe preloading for combined-family replay tests now
  derive protocol types, chain identity, and pool seed ownership from the resolved unique runtime
  target instead of hard-coding Uniswap V2/V3 metadata beside the test harness
- that test-side seed harness is now shared as well:
  the resolved-runtime bootstrap-seed extraction and seed-universe preload helpers live in
  `testing.rs`, so combined-family replay tests and future-family registry fixtures no longer keep
  a second copy of that runtime-target-driven bootstrap seed logic inside `main.rs`
- that verification seam is now exercised beyond Uniswap as well:
  a custom `future_swap` family fixture with its own registry-backed runtime spec now reuses the
  same shared-bootstrap seed extraction path, proving this runtime-target-driven harness is no
  longer tied to the built-in Uniswap family shape
- the config-owned indexer runtime wrapper is slightly less vestigial now too:
  `ResolvedIndexerRuntime` carries the precomputed `protocol_systems` and `dci_protocol_systems`
  views alongside the resolved runtime targets, so `main.rs` no longer has to re-derive those
  startup lists from the target set after config resolution
- that wrapper now owns one more startup boundary as well:
  both the production indexer entrypoint and the `build_all_extractors(...)` test helper delegate
  managed-runner construction through `ResolvedIndexerRuntime::build_managed_runners(...)` instead
  of routing back through a separate `main.rs` runtime-target assembly helper
- the gateway startup boundary is narrower for the same reason:
  production startup now delegates gateway construction through
  `ResolvedIndexerRuntime::build_gateway(...)`, so the runtime-derived protocol-system list used by
  `GatewayBuilder::set_protocol_systems(...)` is no longer re-threaded manually through `main.rs`
- the service-builder startup boundary is now aligned with that same surface too:
  `ResolvedIndexerRuntime::service_config()` yields one config-owned startup view that drives both
  gateway construction and `ServicesBuilder` protocol/DCI wiring, so `main.rs` no longer carries
  separate local copies of those runtime-derived lists while booting the full server
- that service surface now owns the final service-start wiring as well:
  production startup delegates full-server builder assembly through
  `ResolvedIndexerServiceConfig::start_services(...)`, so `main.rs` no longer open-codes the
  `ServicesBuilder` protocol/DCI/handle registration chain before calling `.run()`
- the indexer entrypoint now delegates one layer further into that same surface too:
  `build_all_extractors_for_runtime_targets(...)` no longer open-codes protocol-cache population,
  startup account initialization, and runner construction in `main.rs`; the resolved runtime-target
  set now owns that managed-runner assembly sequence directly
- detected family runtime metadata is narrower now too: `DetectedFamilyRuntime` holds one
  resolved shared-stream object instead of separately carrying duplicated
  `shared_spkg/output_module/durability_scope` strings, so family-runtime detection and runner
  assembly read one shared-stream identity rather than reassembling those fields again at the
  detected-family layer
- that wrapper now stays alive one step longer through the real indexer startup path too:
  `ResolvedIndexerRuntime` carries `ResolvedRuntimeTargets` directly, `build_all_extractors(...)`
  and `build_all_extractors_for_runtime_targets(...)` consume the wrapper instead of immediately
  flattening back to a raw `Vec`, and the collection only unwraps at the final runner fan-out
  boundary where individual runtime targets are actually consumed
- the config boundary now matches that shape too: `ExtractorConfigs::resolved_runtime_targets(...)`
  and its registry-aware variant both return `ResolvedRuntimeTargets`, so callers such as
  `main.rs` and `record_substreams.rs` no longer receive a raw target vector only to wrap it back
  into the family-aware collection API one frame later
- `ResolvedIndexerRuntime` is narrower for the same reason: it now carries just
  `ResolvedRuntimeTargets`, while callers derive protocol-system and DCI-protocol views directly
  from that wrapper instead of caching a second copy of metadata that can drift away from the
  resolved runtime-target set
- family-runtime metadata on `ExtractorConfig` is narrower too: production callsites now resolve
  shared stream target plus family durability scope through one
  `require_resolved_family_runtime_metadata(...)` path instead of separately validating
  `shared_spkg`, `shared_module`, and `durability_scope` through multiple field-specific helper
  methods
- the old runtime-target collection free helpers have now effectively fallen out of the live
  codepath as well: selector lookup, unique-target enforcement, protocol projection, and
  initialized-account request coalescing are exercised through `ResolvedRuntimeTargets` methods in
  both entrypoints and unit coverage, which narrows the chance that a future family integration
  reintroduces a parallel slice-based collection API by accident
- the repo-combined recorder helper surface is less coupled to the binary config module as well:
  library test helpers derive the combined family name plus canonical output module from the
  checked-in top-level `family_runtimes` fragment and the shared family registry, which keeps
  `testing.rs` buildable under the library target while still anchoring recorder expectations to
  the same family registry used by the shared runtime
- the shared family registry is a bit more declarative on identity now too: the built-in Uniswap
  family declaration no longer hand-writes its canonical output module, shared stream name, and
  durability scope as three unrelated literals, and registry coverage proves the same canonical
  derivation shape works for a future family as well
- registered family-name inference is narrower now too: `FamilyRuntimeRegistry` owns the
  "protocol systems -> exactly one registered family" check directly, and both shared-bootstrap
  plan inference and family-runtime test helpers now reuse that single registry entrypoint instead
  of open-coding parallel family-membership scans
- managed-runner fan-out is narrower now too: `ResolvedRuntimeTargets::build_managed_runners(...)`
  no longer delegates through separate free-standing
  `build_runner_for_runtime_target(...)` / `build_runners_for_runtime_targets(...)` wrappers, and
  instead drives per-target managed-runner construction directly from the resolved runtime-target
  collection that already owns the shared-family vs standalone boundary
- service/runtime metadata is narrower now too: `ResolvedIndexerRuntime` no longer caches a second
  `ResolvedIndexerServiceConfig` copy beside `runtime_targets`, and instead derives service-layer
  protocol-system / DCI-protocol views on demand from the resolved runtime-target set that already
  owns the authoritative family-vs-standalone composition
- that wrapper is thinner in orchestration shape too: `ResolvedIndexerRuntime` no longer forwards
  managed-runner construction as its own async entrypoint, and callers now explicitly split
  `(runtime_targets, service_config)` before building runners, which keeps the actual shared-family
  startup path anchored on `ResolvedRuntimeTargets`
- the config/runtime boundary is narrower one step further now too: the live indexer startup path
  no longer depends on a separate `ResolvedIndexerRuntime` wrapper at all, and instead resolves
  `ResolvedRuntimeTargets` directly from `ExtractorConfigs` while deriving
  `ResolvedIndexerServiceConfig` from that same target set for gateway/service startup
- the recorder side path is narrower now too: `record_substreams.rs` no longer reimplements its
  own target-selection and request-override flow on top of raw runtime-target iteration, and
  instead asks `ResolvedRuntimeTargets` to resolve either a selected family/standalone target or a
  unique default target and to materialize the final substreams execution request from that same
  shared runtime-target API
- the startup/request-preparation path is narrower now too: runner startup no longer reaches out to
  separate top-level helpers to initialize runtime-target accounts or to prepare a family stream
  request, and instead calls `ResolvedRuntimeTargets::initialize_accounts(...)` plus
  `ResolvedFamilyRuntime::prepare_substreams_request(...)`, keeping those pre-stream lifecycle
  steps attached to the family-aware runtime types that already own shared bootstrap and shared
  stream semantics
- the family-owned startup assembly boundary is narrower now too: branch extractor construction,
  family request preparation, prepared shared-stream loading, and the final
  `PreparedFamilyRunnerStartup -> ManagedRunner::Family` assembly step now live in
  `extractor/family_managed_startup.rs`, so `runner.rs` no longer owns that end-to-end family
  startup sequence internally before the shared stream begins
- that same family startup path is one layer flatter now as well: `ResolvedRuntimeTarget` no
  longer routes family startup through the generic `PreparedRuntimeTargetRequest` enum before it
  can become a managed runner startup, and instead calls
  `prepare_family_managed_startup(...)` directly, leaving the request wrapper in `runner.rs`
  as a standalone-only concern while the shared-family path stays entirely inside
  `family_managed_startup.rs`
- the generic runtime-target startup wrapper has now fallen out of the live path entirely:
  standalone startup mirrors the family shape through
  `extractor/standalone_managed_startup.rs`, so both `ResolvedRuntimeTarget::Standalone(...)` and
  `ResolvedRuntimeTarget::Family(...)` now resolve directly to managed-startup artifacts without a
  shared `PreparedRuntimeTargetRequest` enum sitting in `runner.rs`
- those startup modules are flatter internally now too: both
  `family_managed_startup.rs` and `standalone_managed_startup.rs` now construct the final startup
  artifact directly instead of first materializing a module-local `Prepared*RuntimeTargetRequest`
  wrapper and then converting it in a second step
- family final assembly is narrower now too: `FamilyExtractorRunner::new(...)` now lives beside
  the family managed-startup path in `family_managed_startup.rs`, so the protocol-system keyed
  subscription-index initialization and final family-runner construction no longer have to stay
  in `runner.rs`
- standalone/member startup now follows that lifecycle split more closely too:
  standalone bootstrap-completion and resume-start decisions no longer live as an inlined block
  inside `ExtractorBuilder::prepare_substreams_request(...)`, and instead flow through
  `extractor_lifecycle.rs`, so both the shared-family path and the standalone/member path now
  consume explicit lifecycle helpers before the final runner assembly step
- auxiliary decode ownership is narrower now too: `ProtocolExtractor` no longer reaches into the
  built-in default family registry to discover auxiliary protocol-message decoders on its own, and
  instead consumes the decoder set injected by the builder/runtime layer, keeping future-family
  extension pressure on the shared runtime surface rather than reintroducing protocol-registration
  logic inside the extractor core
- family dynamic-admission fallback is narrower now too: the "unknown component -> hydrate from
  protocol cache -> retry dispatch" flow for shared-family streams no longer lives as a runner-only
  recovery path, and instead hangs off `FamilyBlockChangesDispatcher` itself, so family branch
  routing owns both its seeded membership snapshot and its late component-admission retry behavior
- family runtime-state assembly is narrower now too: `prepare_family_managed_startup(...)` now
  constructs the protocol-cache-backed `FamilyRuntimeState` directly inside
  `extractor/family_managed_startup.rs`, so `runner.rs` no longer has to stitch dispatcher/cache
  state together while converting a prepared family startup into `ManagedRunner::Family`
- that same seam is now regression-backed after the constructor change as well: the protocol-system
  keyed family startup/alias-handling tests compile and pass against the family-owned runtime-state
  builder, which proves the latest refactor did not reintroduce `runner.rs` ownership over the
  shared-family startup state machine
- the family startup handoff is flatter now too: `family_managed_startup.rs` exposes one
  `build_family_managed_runner(...)` entrypoint that owns the internal
  `prepare_family_managed_startup(...) -> build_family_managed_runner_from_startup(...)` sequence,
  so `ResolvedRuntimeTarget::Family(...)` no longer open-codes that two-step startup assembly in
  `runner.rs`
- the generic managed-startup wrapper has narrowed again as well: `ResolvedRuntimeTarget` no
  longer prepares a family/standalone-specific artifact only to wrap it in a
  `PreparedManagedRunnerStartup` enum and immediately unwrap it again, and instead dispatches
  directly into `prepare_*_managed_startup(...) -> build_*_managed_runner_from_startup(...)`
  per target kind
- that removal matters for future family extensibility too: adding another family-capable target no
  longer requires extending a generic startup enum in `runner.rs`, because the shared-family path
  and standalone path each own their own startup artifact and final assembly boundary directly
- family runner test ownership has started to follow the same boundary too: the first
  shared-family dispatch/failure behavior slice now lives under
  `extractor/runner/test/runner_family_tests.rs` instead of inline in `runner.rs`, so future
  shared-stream behavior coverage can move with the family-owned runtime surface rather than keep
  inflating the generic runner file
- that family-owned runner test slice is broader now too: reconnect handling, restart-style
  component/contract follow-up routing, dispatcher protocol-cache preseed coverage, and
  runtime-state hydration coverage all now live in the same `runner/test/runner_family_tests.rs`
  submodule, which materially shrinks the inline shared-family behavior surface still sitting in
  `runner.rs`
- family lifecycle coverage is starting to follow that boundary as well: aligned resume-position
  resolution, bootstrap-skip/materialization preflight, and post-bootstrap shared-stream request
  shaping now live in `extractor/runner/test/runner_family_lifecycle_tests.rs` instead of inline
  in `runner.rs`, so the remaining shared-family inline surface is increasingly limited to
  execution-config derivation and a smaller tail of bootstrap/membership semantics
- that remaining planning surface is shrinking too: shared execution-config derivation, protocol
  alias handling, conflict validation, and family membership/progress invariants now live in
  `extractor/runner/test/runner_family_planning_tests.rs`, leaving the inline `runner.rs` family
  test tail focused much more narrowly on bootstrap-application semantics and a few residual
  runtime-adjacent assertions
- the bootstrap-application tail is no longer inline either: shared bootstrap split/dispatch,
  completed-family skip handling, missing-branch rejection, and shared bootstrap completion-state
  invariants now live in `extractor/runner/test/runner_family_bootstrap_tests.rs`, so the
  remaining family-specific surface in `runner.rs` is now largely limited to a smaller set of
  runtime-metadata assertions rather than end-to-end shared-family lifecycle coverage
- that remaining runtime-metadata tail has now moved out too: family-runtime shared-stream field
  exposure and resolved family-runtime metadata validation now live in
  `extractor/runner/test/runner_family_runtime_metadata_tests.rs`, leaving `runner.rs` much
  closer to a pure orchestration surface with family coverage delegated to narrowly scoped
  submodules under `extractor/runner/test/`
- ownership for the corresponding runtime metadata has narrowed in the implementation too:
  `FamilyRuntimeConfig`, resolved shared-stream metadata derivation on `ExtractorConfig`, and the
  family-only merged `substreams_params` conflict helpers now live in
  `extractor/family_runtime.rs`, so `runner.rs` keeps the generic config struct but no longer
  owns the family-specific metadata resolution rules that feed the shared bootstrap/shared-stream
  planner
- generic extractor construction is less family-aware now too: `ExtractorBuilder::new(...)` no
  longer reaches into the built-in default family registry to discover auxiliary protocol-message
  decoders implicitly, and instead the startup/runtime layer injects standalone default decoders
  explicitly while shared-family startup injects the resolved family decoder set, which keeps
  future-family extension pressure on the runtime planning/startup boundary instead of the generic
  builder
