# Uniswap V2/V3 Shared Bootstrap And Combined Substream Plan

## Status

- Phase 1: complete
- Phase 2: complete
- Phase 3: in progress

Current Phase 3 operator entrypoint:

- `scripts/check-combined-family.sh` is now the top-level validation surface
  - `command acceptance` / `run-acceptance` now mean the repo-local shared-runtime acceptance
    surface: the manifest-backed extensibility contract gate plus the DB-backed shared-runtime
    gate
  - `command full` / `run-full` now mean repo acceptance plus live Fynd environment validation
    rather than the looser `all` mode: `main.rs` now locks `full` to
    `extensibility gate -> DB gate -> live gate`, while `all` remains the narrower
    `DB gate -> live gate` operator surface
  - `doctor` now also reports the canonical combined-family indexer operator readiness and
    startup command surface from `scripts/run-combined-family-indexer.sh`, while keeping
    `acceptance_ready` / `full_ready` scoped to the acceptance-vs-live split:
    `acceptance_ready` becomes true once the extensibility and DB gates are ready, while
    `full_ready` stays false until the live Fynd gate is ready too
  - the top-level live step is no longer locked to the full two-test Fynd pass: setting
    `TYCHO_COMBINED_FAMILY_LIVE_SELECTION=route|settlement|all` narrows `command live`,
    `run-live`, `command full`, and `run-full` without changing the default `all` behavior
- `scripts/check-combined-family-db.sh` remains the repo-local DB-backed gate
- `scripts/check-combined-family-fynd-live-e2e.sh` remains the live Tycho/Fynd gate
  - the live gate is now source-anchored too: its default route/settlement ignored-test
    selectors live in
    `crates/tycho-indexer/tests/combined_family_live_gate.tests`, while
    `FYND_E2E_ROUTE_TEST`, `FYND_E2E_SETTLEMENT_TEST`, and
    `TYCHO_COMBINED_FAMILY_LIVE_TEST_MANIFEST` still allow explicit override when needed
  - `main.rs` now also locks that manifest to the canonical route-return and settlement test
    names by default, so the live gate cannot silently drift away from the intended two-test
    Fynd contract unless an operator explicitly uses the documented override surfaces
  - the live gate doctor is stricter now too: beyond `/v1/health`, it also requires the local
    Tycho RPC to return queryable `uniswap_v2` and `uniswap_v3` protocol components before
    reporting live readiness, so managed and manual live runs do not race ahead on a merely
    healthy-but-not-yet-queryable combined-family instance
  - inherited empty `TYCHO_STREAM_WS_BUFFER_SIZE` and
    `TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE` values are now scrubbed with `env -u` before the
    live Fynd test command is launched, so shell-exported empty overrides cannot silently poison
    the managed or manual combined-family live gate
- combined-family V3 auxiliary decoding now reloads full tip state for existing pools instead of
  only the current block's touched keys:
  `ProtocolExtractor::get_protocol_states_at_tip(...)` merges DB state with reorg/in-flight
  overlays, and
  `test_get_protocol_states_at_tip_preserves_full_tick_map_across_swap_only_updates`
  proves swap-only V3 updates no longer discard previously known `ticks/*/net-liquidity`
  attributes from the in-memory runtime state
- combined-family auxiliary runtime hooks now include an extensible chain-state hydrator
  registry:
  family defaults expose protocol-scoped `AuxiliaryProtocolStateHydrator` entries alongside
  auxiliary message decoders, managed and standalone startup paths thread those hydrators into
  `ProtocolExtractor::new_with_runtime_support(...)`, and the first concrete Uniswap V3 hydrator
  reuses the existing bootstrap snapshot helpers to backfill created pools that would otherwise
  land with non-zero liquidity but no tick map
- `scripts/check-combined-family.sh` now also exposes optional managed live/full modes:
  `run-live-managed` and `run-full-managed` can start the canonical combined-family indexer
  entrypoint, wait for live combined-family query readiness, run the live Fynd E2E contract, and
  then tear the managed indexer process down again
  - `managed_live_ready` / `managed_full_ready` are intentionally stricter than
    `operator_ready`: they now require the live Fynd prerequisites too, so the aggregate doctor
    output no longer reports the managed path ready when the indexer could boot but the sibling
    Fynd repo, its `e2e_quote` test, or basic local curl-based health probing are unavailable
  - the top-level doctor now also forwards the live-gate source-anchoring fields
    (`live_fynd_repo_exists`, `live_fynd_test_exists`, `live_test_mapping_ready`,
    `live_curl_available`) so managed-path readiness failures can be attributed directly to the
    manifest-backed Fynd test mapping contract instead of looking like an opaque aggregate false
- the checked-in bootstrap asset now converges on the same ownership model too:
  `crates/tycho-indexer/config/shared_uniswap_bootstrap.yaml` is now a materialized
  route-format shared bootstrap file rather than a thin include wrapper, while
  `uniswap_v2_bootstrap.yaml` and `uniswap_v3_bootstrap.yaml` remain only compatibility wrappers
  that include the shared file and rely on protocol-system filtering at parse time; this keeps
  legacy paths working without preserving duplicated bootstrap seed lists
- the checked-in shared-stream param assets now follow that same ownership split:
  `crates/tycho-indexer/config/shared_uniswap_substreams.yaml` is now the canonical
  materialized route-format source for family-wide pool filters, while
  `uniswap_v2_substreams.yaml` and `uniswap_v3_substreams.yaml` remain compatibility/member
  wrappers on top of it; only the V3-specific `factory` filter remains member-local, so
  family-wide route ownership no longer lives in per-protocol substreams files
- family registration now also exposes a first canonical identity constructor for default
  registry authors:
  `family_registry::canonical_shared_family_runtime_spec!` derives the canonical shared output
  module, shared stream name, and durability scope directly from a family name literal, and the
  built-in Uniswap default registry now uses that macro instead of hand-writing those three
  identity fields; this narrows the amount of registry-local boilerplate future family additions
  need to repeat before they can reuse the shared bootstrap and single-stream runtime path
- protocol-scoped auxiliary runtime hooks can now live on the member declaration itself:
  `FamilyMemberSpec` owns optional auxiliary message decoders and chain-state hydrators,
  runtime-default resolution prefers those member-scoped hooks and only falls back to filtered
  family-level hooks when a member does not override them, and the built-in Uniswap defaults now
  source the V3-specific auxiliary decoder/hydrator wiring directly from the V3 member
  registration; this removes another family-local special case from the extensibility surface

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
- config-path startup coverage now also owns the aliased family-defaults resume paths directly:
  the `main.rs` high-level alias-family regressions for shared family cursor resume and completed
  shared-bootstrap fresh start now build through the config-path runtime owner rather than loading
  `ExtractorConfigs` and bypassing that owner surface; those DB-backed tests also pin an explicit
  family `durability_scope` per test fixture so persisted shared-family state cannot leak across
  repeated local test invocations while still exercising the shared-state contract end-to-end
- the last remaining outer-layer config-introspection path now also goes through that owner:
  `main.rs` no longer directly calls `ExtractorConfigs::from_yaml(...)` outside `config.rs` just
  to inspect inherited family defaults, and instead reads those assertions back through
  `LoadedIndexerRuntimePlan`, so the high-level config-path contract now consistently treats the
  loaded runtime-plan owner as the authoritative entrypoint for config loading, inherited shared
  bootstrap defaults, and managed runner startup
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
- the generic family registry no longer owns the built-in Uniswap V2/V3 registration payload
  directly: the default family registry now lives in a dedicated extractor module, so future
  built-in families can be added by extending the default registry surface without re-coupling
  the core family-spec types/helpers to Uniswap-specific bootstrap or auxiliary-decoder wiring
- resolved runtime planning now also exposes one unified runtime-target surface for the indexer
  entrypoint, so `main.rs` no longer needs separate family and standalone planning passes before
  it starts building runners
- runtime-target startup now also owns managed runner materialization for both family and
  standalone targets: `runtime_targets_startup.rs` remains the single fan-out boundary that turns
  prepared startup state into managed runners and extractor handles, while both
  `PreparedFamilyRunnerStartup` and `PreparedSingleRunnerStartup` now own their own final
  `prepared startup -> managed runner` assembly step instead of leaving standalone startup behind
  a separate runner-construction helper
- family runtime registration now also owns the shared-bootstrap branch metadata for each member
  protocol, so stream-family membership and bootstrap-family membership no longer drift through
  separate hard-coded member lists
- family member registration now models shared-bootstrap support as one atomic capability object
  instead of three loosely related optional fields, so future protocol families cannot represent
  partial bootstrap handler declarations that only fail much later during runtime setup
- shared-bootstrap member authoring is narrower now too:
  `extractor/family_registry.rs` exposes one-step helpers such as
  `shared_family_member_with_bootstrap(...)` and
  `canonical_pool_list_shared_family_member_spec(...)`, so built-in families and future-family
  tests no longer need to hand-assemble `Some(shared_bootstrap_member_runtime(...))` wrappers
  just to declare a standard bootstrap-capable family member
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
- path-level runtime-plan loading now has one owner surface too:
  `LoadedIndexerRuntimePlan::from_yaml[_with_registry](...)` now owns the
  `extractors config path -> validated config` transition, and its
  `resolved_runtime_plan()` method owns the final borrow-scoped planning step, so production
  startup and config-path test helpers no longer each re-implement their own load-then-resolve
  sequence
- the record-substreams tooling path now uses that same config-owning surface too:
  config-driven request derivation no longer loads extractor YAML and resolves a shared-family
  Substreams request as two caller-owned steps inside `record_substreams.rs`; it now goes through
  `LoadedIndexerRuntimePlan::{from_yaml,resolve_substreams_execution_request}`, which keeps the
  live CLI/tooling path on the same path-level owner model as production startup and config-path
  runtime tests
- startup-oriented mixed-target tests now enter one layer higher too:
  they resolve `ResolvedIndexerRuntimePlan` first and only project down to
  `ResolvedRuntimeTargets` at the final library-internal startup-preparation step, so the test
  seam no longer bypasses runtime-plan resolution when validating mixed family/standalone startup
- config-path startup coverage is narrower now too:
  a default-registry config-path startup helper now owns the common
  `config path -> runtime owner -> runtime plan -> managed runner build` chain for repo Uniswap
  restart-style tests, so those high-level shared-family regressions no longer repeat
  `ExtractorConfigs::from_yaml(...)` before entering the managed-runner startup surface
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
- prepared shared-stream request shaping is now converging on that same resolved-runtime surface
  as well: family and standalone runtimes each own their own
  `prepare_substreams_request(...)` entrypoint, so bootstrap execution, cursor/start-block
  resolution, and final prepared request materialization no longer need a separate
  startup-layer helper selection step outside the resolved runtime model
- top-level startup context assembly is narrower too: the real indexer launch context and the
  shared test launch context now each own their own
  `ResolvedRuntimeTargetsBuildContext` construction, so the upper orchestration layer no longer
  re-lists the full managed-runner startup field set at every call site just to enter the shared
  runtime-target build path
- prepared-startup fan-out is narrower as well: once a runtime target has been prepared, the
  build layer now sees one type-erased prepared managed-startup interface instead of an explicit
  `PreparedRuntimeTargetStartup::{Family,Standalone}` enum, so the batch startup path no longer
  needs a second family-vs-standalone dispatch step just to turn prepared targets into managed
  runners
- managed-runner fan-out is narrower too: the runtime-build path now returns one unified
  `ManagedRunner` wrapper around a managed-runtime interface instead of an explicit
  `ManagedRunner::{Single,Family}` enum, so the production startup path only depends on the
  common runner surface (`run`) while tests inspect concrete runner shape through explicit helper
  accessors instead of relying on enum-based orchestration
- shared-stream startup parameter plumbing is narrower now too:
  `ResolvedRuntimeTargetsBuildContext` now owns `ManagedExtractorBuildContext` assembly and
  `ManagedExtractorBuildContext` owns loading a `SubstreamsStream` from a
  `PreparedSubstreamsRequest`, so family and standalone managed startup paths no longer each
  hand-plumb endpoint/bucket/token/partial-block stream-load parameters through separate local
  `load_stream_for_prepared_request(...)` call sites
- that startup-parameter ownership is now tighter still:
  `final_block_only` also lives on `ManagedExtractorBuildContext`, so family and standalone
  `prepare_managed_startup(...)` entrypoints no longer need a separate out-of-band shared-stream
  launch flag beyond the common managed startup context
- prepared-request materialization ownership is narrower now too:
  `PreparedFamilyRunnerStartup` and `PreparedSingleRunnerStartup` each own their
  `PreparedSubstreamsRequest -> SubstreamsStream -> prepared startup artifact` assembly step, so
  family and standalone managed startup paths can stop open-coding that last shared stream-load
  transition after their protocol-specific bootstrap/progress preparation logic finishes
- prepared-startup ownership is narrower one step further as well:
  both prepared startup artifacts now also own the final `-> ManagedRunner` assembly step, so the
  runtime-target fan-out path no longer carries a standalone-only `build_single_managed_runner...`
  escape hatch while family startup already uses an instance-owned conversion
- prepared-startup to managed-runner fan-out is narrower now too:
  once runtime targets have been prepared, `PreparedRuntimeTargetStartup` and
  `PreparedRuntimeTargetsStartup` now build managed runners through a synchronous contract rather
  than an `async_trait`-based async wrapper, because the remaining work at that stage is pure
  in-memory runner assembly and no longer performs protocol-specific I/O
- runtime-target startup dispatch is narrower one step further too:
  `ResolvedRuntimeTarget::prepare_managed_startup(...)` now matches directly on family vs
  standalone resolved targets and converts their typed prepared-startup artifacts into the boxed
  `PreparedRuntimeTargetStartup` wrapper through `From<...>` conversions, so
  `runtime_targets_startup.rs` no longer carries a second internal async trait layer just to
  rewrap target-local startup calls before the final prepared-startup fan-out
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
- that recorder ownership split is tighter now too:
  the default CLI path in `record_substreams.rs` resolves derived shared-family requests through
  `ExtractorConfigs::{from_yaml,resolve_substreams_execution_request}` directly, while the
  registry-parameterized path is left for explicit custom-family tests and tooling; this keeps
  the default operator facade on the same config/runtime-target owner surface as production
  indexing instead of routing even the built-in family through the custom-registry test seam
- shared runtime planning now reaches the startup test surface through that same owner too:
  `build_all_extractors_for_tests(...)` no longer bypasses `ResolvedIndexerRuntimePlan` and jump
  straight from `ExtractorConfigs` into `ResolvedRuntimeTargets`; it now resolves the
  registry-aware runtime plan first and asks that plan to build managed runners, which keeps the
  shared-family startup tests on the same planning facade used by production indexing
- config-path shared-bootstrap tooling now uses that same owner surface too:
  `shared_bootstrap_seed_universe_spec_from_config_path_with_registry_for_tests(...)` now
  resolves a `ResolvedIndexerRuntimePlan` and consumes its unique runtime target instead of
  reopening `resolved_runtime_targets_with_registry(...)` directly; the helper also now makes the
  existing `'static` custom-registry requirement explicit at its signature, matching the current
  runtime-plan ownership model instead of hiding that lifetime constraint behind a looser helper
  boundary
- runtime-plan metadata checks now stay on the runtime-plan facade too:
  `ResolvedIndexerRuntimePlan` now exposes direct `protocol_systems()` and
  `dci_protocol_systems()` accessors, and the surviving runtime-plan tests read protocol metadata
  through that facade instead of unpacking `runtime_targets` just to project the same family-aware
  protocol list back out of a lower layer
- config-path custom-registry startup tests now converge one step further too:
  the future-family managed-runner startup tests in `main.rs` no longer open-code
  `from_yaml_with_registry(...)` before calling the shared test startup surface; they now enter
  through one `build_all_extractors_from_config_path_with_registry_for_tests(...)` helper that
  keeps the “config path -> config load -> runtime plan -> managed runner build” chain under one
  owner surface for custom-family startup coverage
- the production task-launch seam is narrower now too:
  `run_indexer(...)` and `run_spkg(...)` now resolve `ResolvedIndexerRuntimePlan` before entering
  `create_indexing_tasks(...)`, and that helper now accepts the runtime plan directly instead of
  taking raw `ExtractorConfigs` and silently re-deriving the shared-family plan internally; this
  keeps the binary entrypoint aligned with the same explicit “config -> runtime plan -> launch”
  ownership model used elsewhere in phase 3
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
  `ResolvedRuntimeTargets::prepare_startup(...)` now prepares family and standalone startup
  artifacts into separate typed collections before the final runner fan-out, so the orchestration
  surface that turns runtime targets into managed runners no longer depends on a generic
  family-or-standalone startup enum in the live path
- shared runtime-target startup environment is narrower now too:
  protocol-cache population plus initialized-account preload for resolved runtime targets now live
  in `extractor/startup.rs` via a dedicated `prepare_startup(...)` step, so
  startup ownership stays on `extractor/startup.rs` instead of a separate generic
  managed-startup module that would reintroduce a second abstraction layer between runtime-target
  planning and family-vs-standalone runner construction
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
- the built-in Uniswap family now follows that generic path directly too: its default registry no
  longer installs a no-op family-level bootstrap materializer wrapper, and
  `default_uniswap_family_uses_generic_shared_bootstrap_plan_materializer` locks in that the
  shared bootstrap plan materializer owner for Uniswap is the registry/generic fallback rather
  than a family-specific pass-through hook
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
- the target-dispatch boundary is now proven one layer higher too:
  `build_all_extractors_for_tests(...)` has DB-backed regression coverage proving one top-level
  startup pass can assemble a mixed runtime-target set into exactly one shared-family managed
  runner plus one standalone managed runner, while still preserving alias-shaped public handle ids
  and protocol-system keyed family branches
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
- family lifecycle progress loading is narrower at startup too: resume/cursor/bootstrap decisions
  now hydrate one per-branch progress snapshot and derive consistency/completion checks from that
  shared snapshot shape, instead of re-reading branch progress, cursors, and bootstrap markers
  through separate family lifecycle passes
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
- the outer managed runtime execution shell is now shared as well via
  `extractor/execution_loop.rs`, so runtime-handle selection, spawn/loop control, and tracing
  scaffolding no longer diverge between standalone and family runners even though each runner
  still keeps its own stream/control semantics
- standalone runtime execution is narrower now too: `ExtractorRunner`'s control handling,
  stream-item handling, and block-response handling now live as explicit helpers inside
  `extractor/single_runtime_execution.rs`, which makes the single-runner path structurally mirror
  `family_runtime_execution.rs` and reduces the remaining surface area that still needs a shared
  execution abstraction
- the runtime-step branch contract is narrower now too:
  `extractor/execution_loop.rs` now owns the shared “control action -> continue/stop” and
  “stream action + block number -> continue/stop + tracing fields” mapping helpers, while both
  standalone and family runners expose the same step-level branch shape around those helpers
  instead of each runner re-encoding that exit contract locally
- the runtime-step select skeleton is now shared as well:
  `extractor/execution_loop.rs` now exposes the common `tokio::select!` step macro used by both
  standalone and family runners, so the two runtime paths no longer duplicate the control-vs-
  stream selection shape itself and only provide their protocol-specific branch bodies
- the runtime-loop control-flow type is now shared too:
  standalone and family runners no longer carry separate local `Continue/Stop` enums; both now
  use one execution-layer control-flow type from `extractor/execution_loop.rs`, which removes
  another duplicated runtime contract that future shared-family protocols would otherwise have to
  copy
- the runtime stream-item plumbing is now shared too:
  `extractor/execution_loop.rs` now owns the common `Option<Result<BlockResponse, _>>` handling
  path for shared stream shutdown/error mapping plus shared block-number derivation, while
  standalone and family runtimes only provide their response-specific `BlockResponse ->
  RuntimeLoopControlFlow` handling
- the runtime control-message plumbing is now shared too:
  `extractor/execution_loop.rs` now owns the common `ControlMessage::{Stop, Subscribe}` dispatch
  shell, while standalone and family runtimes only provide their stop logging and subscribe-side
  attachment behavior instead of each runner re-encoding the control-message match locally
- the runtime subscribe payload contract is narrower now too:
  execution-layer control handling now unwraps `Subscribe { extractor_id, sender }` before
  calling runtime-specific subscribe handlers, so standalone and family runners no longer repeat
  local `ControlMessage::Subscribe` destructuring just to reach their attachment logic
- the runtime `BlockResponse` dispatch shell is now shared too:
  `extractor/execution_loop.rs` now owns the common `New` / `Undo` / `Ended` match boundary via a
  shared dispatch macro, while standalone and family runtimes only provide their response-specific
  new-block, revert, and shutdown behavior instead of duplicating the outer response fan-out
- the managed-startup coordination layer is narrower now too:
  `ResolvedFamilyRuntime` / `ResolvedStandaloneRuntime` each own their
  `prepare_managed_startup(...)` path and prepared startup artifacts own their
  `build_managed_runner(...)` step, so `runtime_targets_startup.rs` only orchestrates resolved
  targets instead of carrying family-specific or standalone-specific startup assembly details
- the prepared-startup fan-out is narrower again as well:
  `PreparedRuntimeTargetStartup` now hides prepared family and standalone startup artifacts behind
  one `Send`-safe prepared-runner interface, so the runtime-target startup layer no longer
  re-encodes a second explicit `Family | Standalone` dispatch step just to turn already-prepared
  startup state into managed runners
- protocol-level family defaults are converging on one registry-owned view as well:
  bootstrap lookup, auxiliary decoder lookup, and shared-route alias lookup can now reuse one
  registered protocol-defaults surface instead of separately re-deriving family name, member
  spec, and decoder groups at each call site
- family registration is a bit narrower now too: `extractor/family_registry.rs` now exposes one
  canonical family-registration entrypoint that derives `output_module`, `shared_stream_name`,
  and `durability_scope` from the family name even for production specs with auxiliary decoders,
  so adding a future shared family no longer requires manually repeating those identity fields
- the family-spec contract is narrower as well: `FamilyRuntimeSpec` now exposes constructor/getter
  APIs that own family metadata access, and the registry/planning/bootstrap surfaces plus
  future-family tests no longer depend on that struct's raw field layout, which reduces another
  source of cross-module drift when shared-family registration evolves
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
- alias-member planning and runner wiring regressions now also lock the remaining naming
  invariant more directly: shared-family membership, route filtering, and runtime-target lookup
  continue keying on `protocol_system`, not on extractor `name`, so aliased members do not
  silently fall back to the legacy `extractor name == protocol_system` assumption
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
- extensibility coverage for future families no longer depends on binding a local mock gRPC port:
  `record_substreams_fixture_with_registry_records_future_family_request` now exercises the
  `record-substreams` request-building and fixture-writing path through an injected in-memory
  recorder, so the custom-family acceptance gate remains valid in restricted CI/sandbox
  environments while still proving the family registry drives the correct shared-stream module,
  start/stop range, and merged family params
- shared-runner collapse is now directly covered for future families as well:
  `test_build_all_extractors_managed_startup_collapses_custom_family_into_one_shared_runner`
  proves a custom registry can collapse aliased future-family members into one resolved family
  runtime target while preserving protocol-system keyed branch wiring and alias-shaped operator
  handles, without reintroducing per-protocol runner orchestration at the wiring boundary
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
- future-family extensibility is now covered at the managed-runner operator path too:
  `custom_registry_builds_future_family_managed_runner_and_starts_one_shared_stream` proves a
  custom family can build one managed runner, preserve two logical handles, and start exactly one
  shared Substreams session after a family-scoped bootstrap marker, while
  `custom_registry_resumes_future_family_from_persisted_shared_cursor` proves the same family can
  resume that shared session from persisted family-scoped cursor state without reintroducing
  per-protocol stream startup
- that future-family proof surface is now source-anchored too: the checked-in
  `crates/tycho-indexer/tests/combined_family_extensibility_contract.tests` manifest enumerates
  the minimum registry/shared-bootstrap/decoder/managed-startup/recorder proof set, and
  `main.rs` verifies each listed entry still resolves to a real test function in the expected
  source file
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
- live Fynd startup has one decoder hardening edge case under active investigation:
  zero-liquidity Uniswap V3 pools can legitimately arrive without any
  `ticks/*/net-liquidity` attributes, so the V3 snapshot decoder is being relaxed for that
  empty-pool shape while still treating missing tick maps on non-zero-liquidity pools as an
  actual bad-state signal
- the previously isolated non-zero-liquidity V3 created-pool admission gap is now covered by the
  shared runtime architecture:
  `test_uniswap_v3_created_pool_can_currently_emit_non_zero_liquidity_without_ticks` is kept as
  the baseline proof of the bad raw event shape, while
  `test_uniswap_v3_created_pool_uses_auxiliary_chain_hydrator_when_available` proves the
  combined-family path can now repair that shape before finalizing the created transaction by
  invoking the protocol-scoped auxiliary hydrator registry and merging hydrated
  `ticks/*/net-liquidity` plus balances back into the runtime state
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
- that fixture-backed proof is stricter now too:
  the shared bootstrap test seeding path materializes a minimal persisted V3 state shape, and the
  DB-backed replay/restart assertions now fail if a persisted non-zero-liquidity V3 state loses
  all `ticks/*/net-liquidity` attributes; this keeps the repo-local history-slice gate aligned
  with the same "non-zero liquidity requires a tick map" runtime invariant enforced by the live
  auxiliary hydrator path

### Not Yet Proven Enough To Close The Goal

- automatically exercised live combined-runtime Fynd E2E proof covering route return and
  quote settlement against a local Tycho + live RPC environment
- that remaining live check is now at least operationally standardized:
  `scripts/check-combined-family-fynd-live-e2e.sh` provides `doctor`, `command`,
  `run-route`, `run-settlement`, and `run-all` modes against the sibling `fynd` repository, so
  the only remaining gap is whether to promote that live operator workflow into a formal
  acceptance requirement rather than leaving it as a manual gate
- that live gate is slightly less flaky now too:
  both the route and settlement checks default to `FYND_E2E_HEALTH_MODE=quote_ready`, so the live
  combined-family gate now waits for Tycho/Fynd quote-path readiness and lets the existing quote
  retry loop absorb first-block derived-data warmup instead of blocking the settlement gate on a
  full derived-data pass across the entire seeded universe
- the live gate no longer inherits one known shell-level footgun either:
  empty exported `TYCHO_STREAM_WS_BUFFER_SIZE` or `TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE` values
  are stripped before invoking `cargo test`, so operator shells that previously exported those
  names blank no longer cause spurious live-gate startup drift
- the managed live wrapper is tighter now too:
  if the managed combined-family indexer exits before readiness, immediately after readiness, or
  during downstream live validation, `scripts/check-combined-family.sh run-live-managed` now fails
  fast and tails the managed indexer log instead of degrading into a long health-timeout wait or a
  misleading downstream Fynd connection error
  - that wrapper now also recognizes the current known external blocker directly:
    if the managed indexer log contains `invalid JWT token`, it emits an explicit
    `StreamingFast rejected SUBSTREAMS_API_TOKEN` hint before tailing the log, so the remaining
    live-path failure is classified as an external credential problem rather than a shared-runtime
    correctness issue
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
- late shared-family admission is harder to strand on restart-style drift now too: when branch
  routing fails because only contract/storage ownership is missing, the dispatcher now reuses the
  protocol cache to hydrate by contract address as well as by component id before retrying the
  block-scoped dispatch, so dynamic components already present in cache do not depend exclusively
  on a component-id-bearing follow-up payload to recover their branch routing

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
  completed end-to-end for the current seven-test close-out surface, spanning fixture-backed
  history-slice / restart / reconnect coverage plus the completed-shared-bootstrap fresh-start
  invariant, so the remaining Phase 3 gap is no longer whether the repo-local DB gate runs
  successfully, but whether additional live-environment promotion is warranted beyond that
  repo-backed proof surface
- the DB gate manifest itself is now constrained to keep the strongest shared-session external
  semantics proof in scope: `main.rs` verifies that
  `test_serial_db::combined_family_runner_replays_fixture_backed_v2_and_v3_history_slice_in_one_shared_session`
  remains in `combined_family_db_gate.tests`, so the close-out gate cannot silently drop the
  fixture-backed `/v1/protocol_components` and `/v1/protocol_state` validation while still
  passing the looser "all referenced tests exist" check
- that manifest-backed close-out surface now also locks one more startup invariant directly:
  `test_serial_db::combined_family_runner_alias_members_fresh_start_from_completed_shared_bootstrap`
  remains in `combined_family_db_gate.tests`, so the strict repo-local DB gate also proves that a
  persisted shared bootstrap completion marker makes the top-level shared-family startup begin at
  `bootstrap_block + 1` without reusing a stream cursor, even when member extractor names are
  aliases rather than protocol-system literals
- the DB gate manifest now also locks dynamic-admission proof on both family branches directly:
  `main.rs` verifies that
  `test_serial_db::combined_family_runner_restart_applies_follow_up_state_after_dynamic_component_admission`,
  `test_serial_db::combined_family_runner_restart_applies_v3_follow_up_state_after_dynamic_component_admission`,
  `test_serial_db::combined_family_runner_reconnect_applies_v2_follow_up_state_after_dynamic_component_admission`
  and
  `test_serial_db::combined_family_runner_reconnect_applies_v3_follow_up_state_after_dynamic_component_admission`
  remain in `combined_family_db_gate.tests`, so the strict close-out gate cannot silently drop the
  restart-time or reconnect-time V2/V3 seeded-universe dynamic-admission proof while still
  keeping only the looser history-slice or bootstrap-completion coverage
- that restart-time manifest coverage now keeps the narrower ownership-shape regression too:
  `combined_family_db_gate.tests` includes
  `test_serial_db::combined_family_runner_restart_keeps_dynamic_component_queryable_after_contract_and_storage_only_follow_up`,
  and `main.rs` locks that entry explicitly, so the strict DB gate also proves the shared-family
  restart path keeps a dynamically admitted component externally queryable even when the
  post-restart follow-up block carries only contract-only and storage-only ownership signals
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
  `prepare_family_managed_startup(...)` plus
  `ManagedExtractorBuildContext`, so `build_family_runner(...)` no longer owns the intermediate
  "builders -> keyed extractors -> prepared stream" conversion path
- the runtime-target dispatch surface is narrower too: prepared managed startup assembly now hangs
  off `ResolvedRuntimeTarget::prepare_managed_startup(...)`, so the generic runtime-target
  entrypoint no longer owns a bespoke family-vs-standalone startup branch and instead delegates
  both paths through the resolved runtime target abstraction that already owns shared stream
  metadata and startup-account derivation
- that dispatch surface now owns prepared startup assembly as well:
  the top-level startup path now prepares typed family and standalone startup artifacts directly,
  so it no longer needs a generic managed-startup enum just to converge on one lifecycle before
  immediately splitting back into protocol-family-specific runner construction
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
- that batch startup surface now carries one more phase as well:
  `ResolvedRuntimeTargets::prepare_startup(...)` no longer stops at protocol-cache population plus
  initialized-account preload, and instead also prepares family/standalone startup artifacts
  before the final runner fan-out, so the batch path no longer performs a second
  target-orchestration loop that re-enters target-local startup assembly through a generic wrapper
  after the shared startup preflight has already completed
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
- the test-side startup boundary is narrower too: `main.rs` no longer owns a local
  `build_all_extractors(...)` wrapper just to inject the default family registry for serial DB
  runtime tests, and instead aliases one helper from `testing.rs`, so both production startup and
  the long-lived runtime/family regression harness now route managed-runner assembly through the
  same config/runtime/test support surfaces rather than letting the CLI entrypoint keep a parallel
  startup helper
  from that wrapper instead of caching a second copy of metadata that can drift away from the
  resolved runtime-target set
- family-runtime metadata on `ExtractorConfig` is narrower too: production callsites now resolve
  shared stream target plus family durability scope through one
  `require_resolved_family_runtime_metadata(...)` path instead of separately validating
  `shared_spkg`, `shared_module`, and `durability_scope` through multiple field-specific helper
  methods
- that metadata path is less repetitive at runtime now as well:
  `resolve_family_runtime_metadata(...)` can derive `shared_module` and `durability_scope` from the
  shared family registry when member config only carries the family name plus resolved
  `shared_spkg`, and managed extractor initialization now passes the registry through that same
  path instead of requiring every family member config to materialize the same identity strings
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
- that collection-level startup boundary is less argument-shaped now too:
  `ResolvedRuntimeTargetsBuildContext` carries the shared startup inputs for protocol-cache
  preload, initialized-account hydration, and per-target runner construction, so the production
  indexer entrypoint and the `build_all_extractors(...)` helper no longer have to manually keep a
  parallel 10-argument `build_managed_runners(...)` call surface in sync as the shared-family
  startup contract evolves
- runtime-target startup ownership is narrower now too:
  `ResolvedRuntimeTargetsBuildContext`, `PreparedRuntimeTargetsStartup`, and the
  `ResolvedRuntimeTargets::{prepare_startup,build_managed_runners}` orchestration path now live in
  `extractor/runtime_targets_startup.rs`, so `startup.rs` can focus on shared account
  initialization primitives instead of also owning the family-vs-standalone prepared-startup
  boundary for the combined-family runtime
- the live service-lifecycle tail is narrower now too:
  `ResolvedServiceLaunchConfig::start_managed_server(...)` owns the
  `start_services(...) + shutdown task + server_url` assembly step, so `run_rpc(...)` and
  `create_indexing_tasks(...)` no longer keep their own copies of the final Actix startup/shutdown
  orchestration after the shared-family runners have already been built
- the full live indexer startup path is narrower for the same reason:
  `ResolvedServiceLaunchConfig::start_indexing_tasks(...)` now owns the
  `block number -> chain state -> gateway -> token pre-processor -> managed runners ->
  managed server` sequence for resolved runtime targets, so `create_indexing_tasks(...)` is no
  longer the place where the production Uniswap-family shared bootstrap / single-stream runtime is
  materially assembled step-by-step inside `main.rs`
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
- the batch startup tail is flatter for the same reason:
  `PreparedRuntimeTargetStartup::build_managed_runner(...)` now owns the final
  prepared-startup-to-runner step for both shared-family and standalone paths, and
  `PreparedRuntimeTargetsStartup::build_managed_runners(...)` just iterates one unified prepared
  target list instead of maintaining separate family and standalone prepared-artifact collections,
  so the runtime-target collection no longer re-enters a second shape-specific startup branch once
  the shared startup preflight has already produced the prepared startup artifacts
- that same startup convergence is now directly regression-covered too:
  `test_resolved_runtime_targets_prepare_startup_prepares_family_and_standalone_targets_together`
  proves mixed runtime targets now prepare one unified typed startup list, preserve the expected
  one-family-plus-one-standalone split at the artifact level, and still build the same managed
  runner set from that single prepared-target surface
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
  longer routes the live path through a separate `PreparedManagedRunnerStartup` enum at all, and
  instead the batch startup layer dispatches directly into
  `prepare_*_managed_startup(...) -> build_*_managed_runner_from_startup(...)` per target kind
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
- the remaining family test-fixture scaffolding is narrower now too:
  shared-family runner builders, shared-stream fixture blocks, and reusable Uniswap family test
  config constructors now live in `extractor/runner/test/support.rs` instead of being defined
  inline inside `runner.rs`, which removes another chunk of family-specific test-only helper
  ownership from the generic runner module while keeping the test submodules anchored to one
  dedicated support surface
- the last residual family runner/wiring assertions are converging the same way too:
  DB-backed shared-family durability-isolation coverage, branch flush behavior, alias-subscribe
  routing, protocol-system keyed branch wiring, and family-managed-startup shape checks now live
  in `extractor/runner/test/runner_family_runtime_wiring_tests.rs` instead of remaining inline
  in `runner.rs`, which leaves the generic runner module with materially less shared-family
  behavior coverage embedded directly in its local test body
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
- that startup-owned decoder path is now proven one step more directly too:
  `prepare_standalone_managed_startup_injects_custom_registry_decoders` shows a standalone
  extractor for a custom future-family protocol can be built through the real managed-startup
  path, receive its auxiliary decoder set from the supplied runtime registry, and successfully
  decode the resulting non-`BlockChanges` payload without any extractor-core fallback to the
  built-in Uniswap registry
- the shared-family startup path now has the same proof surface:
  `test_prepare_family_managed_startup_injects_custom_registry_decoders` shows
  `prepare_family_managed_startup(...)` can resolve a custom future-family runtime target, build
  protocol-system keyed branch extractors from the injected registry-owned decoder set, and let a
  branch extractor decode a non-`BlockChanges` auxiliary payload through the real shared-family
  startup path rather than only through standalone startup
- the same cleanup now covers standalone bootstrap execution too: `ExtractorBuilder` no longer
  hard-codes the built-in default family registry when it needs shared-bootstrap planning for a
  standalone/member extractor, and instead consumes an injected family runtime registry from the
  startup layer, so generic builder/bootstrap code does not need baked-in awareness of whichever
  family registry happens to be the repo default
- standalone/member stream preparation is narrower now too: the actual
  bootstrap-decision/bootstrap-execution/request-shaping flow for standalone extractors now lives
  in `extractor/standalone_managed_startup.rs` via
  `prepare_standalone_substreams_request(...)`, while `ExtractorBuilder` only forwards to that
  helper for its test shell path, so generic builder code no longer owns the primary standalone
  bootstrap lifecycle implementation
- standalone startup handoff now mirrors the family path more closely too:
  `standalone_managed_startup.rs` exposes `build_standalone_managed_runner(...)`, so
  `ResolvedRuntimeTarget::Standalone(...)` no longer open-codes a
  `prepare_standalone_managed_startup(...) -> build_standalone_managed_runner_from_startup(...)`
  sequence inside `runner.rs`
- managed runtime-target startup dispatch is narrower now too:
  `ResolvedRuntimeTargets::prepare_startup(...)` plus
  `PreparedRuntimeTargetsStartup::build_managed_runners(...)` now own the remaining
  family-vs-standalone orchestration split, so `runner.rs` no longer owns that decision and the
  live path no longer needs a separate `extractor/managed_runtime_startup.rs` module
- the live startup context seam is narrower too:
  `ResolvedRuntimeTarget::prepare_managed_startup(...)` now assembles its own
  `ManagedExtractorBuildContext` from the shared
  `ResolvedRuntimeTargetsBuildContext + ProtocolMemoryCache` inputs, so
  `ResolvedRuntimeTargets::prepare_startup(...)` no longer needs to know which startup fields are
  required by target-local extractor construction before it can hand work off to family vs
  standalone startup
- that startup context is slightly more complete as one surface now too:
  `final_block_only` has been folded into `ResolvedRuntimeTargetsBuildContext`, so
  `ResolvedRuntimeTargets::prepare_startup(...)` no longer hard-codes a separate trailing stream
  mode argument when handing work off to target-local startup; family and standalone startup now
  derive both extractor-build settings and stream-mode settings from the same shared startup
  context object
- shared stream/startup assembly is narrower now too:
  `PreparedSingleRunnerStartup`, prepared-request stream loading, and the
  `PreparedSubstreamsRequest -> SubstreamsStream` assembly path now live in
  `extractor/managed_stream_startup.rs`, so both `family_managed_startup.rs` and
  `standalone_managed_startup.rs` reuse one startup-stream construction surface instead of
  duplicating `load_substreams_package(...)` and `SubstreamsStream::new(...)` sequences under
  separate runtime paths
- managed extractor initialization is narrower now too:
  the common `ExtractorBuilder` configuration chain for initialized managed extractors now lives
  in `extractor/managed_extractor_initialization.rs`, so family and standalone startup paths no
  longer open-code builder setup, decoder injection, runtime-registry wiring, and
  protocol-system keyed extractor assembly separately before entering the shared-stream runtime
- the residual standalone runner shim on `ExtractorBuilder` is gone now:
  builder-owned runtime state no longer carries stream/bootstrap startup concerns, and the last
  test-only `into_runner(...)`/`set_extractor(...)` style escape hatches have been removed from
  `runner.rs`
- the remaining startup test shell is converged on the same helper surfaces as production:
  standalone runner tests now build through `prepare_standalone_substreams_request(...)` plus
  `extractor/managed_stream_startup.rs`, while protocol-system keyed family test wiring no
  longer depends on `ExtractorBuilder` at all
- the test startup helper surface is narrower too:
  `BuildExtractorsTestContext` now carries the family runtime registry directly, so
  `build_all_extractors_for_tests(...)` is the single test helper entrypoint for both default and
  custom-registry startup coverage instead of splitting that flow across a second
  `build_all_extractors_for_tests_with_registry(...)` wrapper
- Substreams package acquisition is narrower now too:
  `runner.rs` no longer owns S3-backed `.spkg` download, package decode, and endpoint assembly;
  that responsibility now lives in `extractor/substreams_package_loader.rs`, which is reused by
  both managed startup and `record_substreams.rs`
- config/runtime model ownership is narrower now too:
  `ExtractorConfig`, `ProtocolTypeConfig`, `BootstrapConfig`, `BootstrapStrategy`, `DCIType`,
  and `configured_stream_start_block(...)` now live in `extractor/extractor_config.rs`, so
  config parsing, runtime-target planning, bootstrap helpers, and startup code no longer need
  `runner.rs` to act as the owning module for extractor configuration types
- extractor control-surface ownership is narrower now too:
  `ControlMessage`, `MessageSender`, `ExtractorHandle`, `SubscriptionsMap`, and
  `BranchSubscriptionsMap` now live in `extractor/control.rs`, so the shared-family runtime,
  standalone runtime, websocket service, and startup/config wiring no longer depend on
  `runner.rs` as the owner of the cross-cutting extractor control plane
- managed extractor assembly ownership is narrower now too:
  `DCIPlugin`, RPC tracer/DCI construction, post-processor lookup, and `ExtractorBuilder` now
  live in `extractor/managed_extractor_initialization.rs`, so `runner.rs` no longer owns
  protocol-extractor construction or dynamic-contract-indexer assembly and is materially closer
  to a pure single-stream execution surface
- runtime-target request planning is narrower now too:
  selector matching, initialized-account coalescing, and
  `ResolvedRuntimeTarget(s) -> ResolvedSubstreamsExecutionRequest` shaping now live in
  `extractor/runtime_target_planning.rs`, so `family_runtime.rs` can focus more narrowly on
  family metadata, registry behavior, and resolved family execution state
- that request-planning ownership is now less enum-shaped too:
  `ResolvedFamilyRuntime` and `ResolvedStandaloneRuntime` each own their direct
  `substreams_execution_request{,_with_start_block}(...)` helpers, while
  `ResolvedRuntimeTarget` only delegates across them; shared-family and standalone startup no
  longer need to re-wrap resolved runtime structs into the outer target enum just to derive the
  final request shape, which reduces another small but recurring target-orchestration seam for
  future family integrations
- the remaining runtime-target planning fan-out is thinner now as well:
  `selector_label`, `chain`, `extractor_configs`, `protocol_systems`,
  initialized-account request shaping, and direct Substreams request derivation now converge
  behind one private planning-view trait in `extractor/runtime_target_planning.rs`, so the
  outer `ResolvedRuntimeTarget` enum carries less inline family-vs-standalone orchestration and
  future protocol families can reuse the same planning surface without copying another batch of
  target-shape delegation code
- the managed-startup wrapper layer is thinner now too:
  `extractor/runtime_targets_startup.rs` now carries one boxed prepared-startup interface for the
  final managed-runner fan-out, while the test surface still preserves family-vs-standalone
  visibility through an explicit prepared-startup kind probe instead of forcing the live startup
  path to keep another concrete enum dispatch seam
- the record-substreams command layer is thinner now too:
  derived request resolution no longer re-runs runtime-target selection after reading the default
  request shape; `record_substreams.rs` now resolves one runtime target, derives its effective
  start block through the shared runtime-target helper surface, and applies request overrides
  directly on that resolved target, which keeps more of the shared-family vs standalone request
  shaping logic inside the runtime-target abstraction instead of in the CLI orchestration path
- the production indexer entrypoint is thinner now too:
  `ExtractorConfigs` can now resolve one explicit `ResolvedIndexerRuntimePlan` owner that binds
  resolved runtime targets and the derived service protocol views together, so `main.rs` no
  longer manually performs the two-step `resolved_runtime_targets -> service_config` assembly
  before launching indexing tasks and future runtime-target orchestration changes have one
  narrower handoff surface at the config/entrypoint boundary
- that same handoff is now narrower at launch time as well:
  `ResolvedServiceLaunchConfig` now accepts the full `ResolvedIndexerRuntimePlan` through one
  `start_indexing_runtime_plan(...)` entrypoint, so `main.rs` no longer has to unpack
  `service_config + runtime_targets` just to pass them straight back into config-owned startup
  orchestration
- the shared startup build-context surface is narrower now too:
  production indexer startup and test-only extractor assembly no longer each open-code
  `ResolvedRuntimeTargetsBuildContext` field-by-field; both now construct it through the same
  `ResolvedRuntimeTargetsBuildContext::new(...)` entrypoint, which reduces drift risk between
  the real indexer path and the shared-family startup/test scaffolding that exercises the same
  managed runner assembly contract
- the config/entrypoint view of runtime targets is narrower now too:
  `ResolvedRuntimeTarget` now exposes explicit `family()` / `standalone()` accessors, and the
  config-level combined-runtime assertions have switched to those accessors instead of directly
  pattern-matching the outer enum shape, which trims another small family-vs-standalone leak from
  the top-level config/runtime-plan boundary that future protocol families should not have to
  duplicate
- the runtime-target managed-startup seam is narrower now too:
  `runtime_targets_startup.rs` no longer inlines the family-vs-standalone startup wrapping logic
  directly inside `ResolvedRuntimeTarget::prepare_managed_startup(...)`; instead, the outer target
  delegates through one private managed-startup view surface implemented by the concrete family and
  standalone runtime owners, which keeps another piece of startup orchestration aligned with the
  same “owner-side behavior, thin target wrapper” direction as request planning
- the managed extractor initialization seam is narrower now too:
  `ManagedExtractorBuildContext` now owns `build_initialized_extractor(...)` and
  `build_protocol_system_keyed_extractors(...)`, so family and standalone managed-startup paths no
  longer call free functions that re-thread the same context fields by hand when assembling
  initialized extractors; the startup surface is slightly closer to one context-owned extractor
  initialization contract that future shared-family runtimes can reuse directly
- the runtime-target module boundary is narrower now too:
  `runtime_target_planning.rs` now re-exports the resolved runtime-target types and request shape
  that it already owns behavior for, and several production entrypoints (`config.rs`,
  `record_substreams.rs`, `startup.rs`, `runtime_targets_startup.rs`) now import those target-side
  types from the runtime-target module instead of reaching through the broader `family_runtime.rs`
  aggregation surface, which reduces the next extraction step when those definitions move farther
  out of the legacy family-runtime bucket
- the family-runtime metadata boundary is narrower now too:
  `FamilyRuntimeConfig`, `SharedStreamTarget`, `ResolvedFamilyRuntimeMetadata`, and the
  `ExtractorConfig` helpers that resolve/attach family-runtime metadata now live in
  `extractor/family_runtime_metadata.rs`, while `family_runtime.rs` keeps compatibility re-exports;
  that means one real slice of configuration/metadata ownership has moved out of the legacy
  family-runtime aggregation file instead of merely being accessed through thinner imports
- single-stream runtime execution ownership is narrower now too:
  the standalone Substreams loop, partial-block handling, subscriber propagation, and
  `ExtractorRunner` now live in `extractor/single_runtime_execution.rs`, so `runner.rs`
  is reduced to managed-runner composition plus the family-runtime re-export instead of owning
  both the single-stream control loop and the combined-family orchestration surface
- the standalone runtime-loop shape is narrower now too:
  `single_runtime_execution.rs` now mirrors the family path's internal `runner + loop_state`
  split, with the extractor-facing runtime resources staying on `ExtractorRunner` and the
  select-loop state moving into a dedicated `SingleRuntimeLoopState`; that removes another
  structural mismatch between standalone and family execution before the remaining shared-loop
  extraction work
- family managed-startup ownership is narrower now too:
  `family_lifecycle.rs` no longer owns the `ResolvedFamilyRuntime ->
  PreparedSubstreamsRequest` startup assembly surface; shared bootstrap gating, resume-position
  resolution, and shared-stream request shaping are now pulled together under
  `extractor/family_managed_startup.rs`, leaving `family_lifecycle.rs` focused on family progress
  consistency and bootstrap/resume state rules instead of also acting as a half-startup owner
- family dispatch ownership is narrower now too:
  component/contract protocol ownership state, dynamic component admission, and protocol-cache
  hydration now live behind `extractor/family_dispatch_registry.rs`, so
  `extractor/family_dispatch.rs` is no longer the owner of both branch-splitting semantics and
  the mutable routing-registry state for shared-family follow-up updates
- family dispatch payload ownership is narrower now too:
  `BlockScopedData <-> BlockChanges` payload validation, decode, branch-payload rewrap, and
  component/contract reference extraction now live in `extractor/family_dispatch_payloads.rs`,
  so `extractor/family_dispatch.rs` can focus more narrowly on family branch splitting rather
  than also owning the raw shared-stream payload plumbing
- family dispatch split-core ownership is narrower now too:
  transaction/storage branch routing, same-block dynamic admission handling, and empty-branch
  block shaping now live in `extractor/family_dispatch_splitter.rs`, so
  `extractor/family_dispatch.rs` is reduced further toward a thin shared-family dispatch surface
  over three narrower owners: routing registry state, shared-stream payload plumbing, and the
  transaction-level branch-splitting core
- family runtime metadata/config ownership is narrower now too:
  registry-backed family/member lookup, shared-runtime metadata lookup, shared-stream identity
  resolution, route-filter normalization, and family-runtime config defaulting now live in
  `extractor/family_runtime_metadata.rs`, while `family_runtime.rs` keeps the bootstrap planning
  and execution surface plus the private `DetectedFamilyRuntime` construction seam
- family shared-bootstrap registry/planning ownership is narrower now too:
  shared-bootstrap capability validation, inferred-family resolution for bootstrap plans,
  family-level bootstrap plan construction, branch-runtime resolution, and bootstrap-strategy
  dispatch now live in `extractor/family_bootstrap_registry.rs`, leaving `family_runtime.rs`
  with less registry policy and more focus on detected family/runtime-plan assembly
- family runtime detection/plan-assembly ownership is narrower now too:
  explicit family detection, shared-stream eligibility checks, family member resolution,
  runtime-plan assembly, and resolved family execution settings now live in
  `extractor/family_runtime_planning.rs`, while `family_runtime.rs` is reduced further toward
  core family runtime types, small shared helpers, and test-local scaffolding
- family shared-bootstrap runtime ownership is narrower now too:
  the shared-bootstrap parser/materializer function types, resolved branch-runtime list, and
  `ResolvedSharedBootstrapExecution` now live in `extractor/family_bootstrap_registry.rs`
  alongside the registry methods that validate member bootstrap support and resolve family-level
  bootstrap execution; `family_runtime.rs` now keeps compatibility re-exports while production
  owners such as `family_registry.rs`, `family_uniswap.rs`, `shared_bootstrap.rs`, and
  `family_lifecycle.rs` depend on the narrower bootstrap owner directly
- family defaults/metadata entrypoint ownership is narrower now too:
  `default_family_runtime_registry()` now lives in `extractor/family_registry.rs`, while
  `FamilyRuntimeConfig` and shared-route protocol canonicalization now live under
  `extractor/family_runtime_metadata.rs`; production entrypoints such as `config.rs`,
  `extractor_config.rs`, `main.rs`, `shared_bootstrap.rs`, and the auxiliary-message decoder tests
  now depend on those narrower owners directly instead of reaching through the broader
  `family_runtime.rs` aggregation surface for default registry/bootstrap-metadata helpers
- family registry/spec type ownership is narrower now too:
  `FamilyMemberSpec`, `FamilyRuntimeSpec`, and `FamilyRuntimeRegistry` now live in
  `extractor/family_registry.rs` alongside the canonical Uniswap family defaults and registry
  builders, while `family_runtime.rs` keeps compatibility re-exports plus higher-level detected
  runtime / execution-plan shapes; this puts the family-definition data model back under the same
  owner as the default family catalog instead of leaving the catalog in `family_registry.rs` and
  the registry/spec types in the legacy aggregation module
- runtime-target planning ownership is narrower now too:
  `ResolvedRuntimeTargets`, `ResolvedRuntimeTarget`, `ResolvedRuntimeTargetSelector`,
  `ResolvedStandaloneRuntime`, `ResolvedSubstreamsExecutionRequest`, and
  `ResolvedInitializedAccountsRequest` now live in `extractor/runtime_target_planning.rs`
  alongside the substreams-request and initialized-account planning logic that actually uses
  them; `family_runtime.rs` now keeps compatibility re-exports while startup wiring moves onto
  the narrower owner directly
- family planning/execution shape ownership is narrower now too:
  `DetectedFamilyRuntime`, `FamilyRuntimeBuildPlan`, `ResolvedFamilyRuntime`,
  `ResolvedFamilyExecutionConfig`, and `ResolvedFamilyRuntimePlan` now live in
  `extractor/family_runtime_planning.rs` next to the detection, membership validation, shared
  stream convergence checks, and resolved execution-plan assembly that produce them; the legacy
  `family_runtime.rs` surface is reduced further toward shared-stream primitives, small merge
  helpers, compatibility re-exports, and test scaffolding
- shared-stream metadata ownership is narrower now too:
  `ResolvedSharedFamilyStream`, `FamilySharedStreamIdentity`, and
  `FamilySharedRuntimeMetadata` now live in `extractor/family_runtime_metadata.rs` beside the
  registry-backed shared-stream identity resolution logic that constructs them, while
  `FamilyRuntimeRegistry::detected_family_runtime(...)` and the small detected-family accessors
  now live in `extractor/family_runtime_planning.rs`; this leaves `family_runtime.rs` even closer
  to a compatibility aggregation surface instead of a mixed owner for stream metadata, detected
  runtime planning, and test helpers
- production entrypoints depend less on the compatibility aggregation surface now too:
  config loading, record-substreams planning, managed startup wiring, shared bootstrap planning,
  runtime-target startup, and extractor initialization now import `FamilyRuntimeRegistry`,
  `ResolvedFamilyRuntime`, `ResolvedStandaloneRuntime`, and related shared-family types from their
  narrower owners (`family_registry.rs`, `family_runtime_planning.rs`,
  `runtime_target_planning.rs`, `family_runtime_metadata.rs`) instead of routing those production
  dependencies through `family_runtime.rs`; the legacy module is correspondingly closer to a test
  and compatibility facade than a real owner used throughout the runtime path
- startup request and default-decoder helper ownership is narrower now too:
  `PreparedSubstreamsRequest` plus the shared
  `ResolvedFamilyRuntime::prepare_substreams_request(...)` /
  `ResolvedStandaloneRuntime::prepare_substreams_request(...)` startup-request shaping logic now
  live in `extractor/managed_substreams_request.rs`, while
  `default_auxiliary_protocol_message_decoders_for_protocol_system(...)` now lives in
  `extractor/protocol_message_registry.rs` beside the auxiliary decoder registry defaults it reads;
  `family_runtime.rs` only keeps compatibility re-exports for those surfaces, which reduces it
  further toward a compatibility facade instead of a mixed owner of startup request assembly and
  protocol-message decoder selection
- production startup / planning paths now route through those narrower owners too:
  family and standalone startup now both call their resolved-runtime
  `prepare_substreams_request(...)` methods from the new
  `extractor/managed_substreams_request.rs` owner, while the default auxiliary decoder helper from
  `extractor/protocol_message_registry.rs`, family startup now imports
  family runtime planning now takes its registry/spec types from `family_registry.rs`, and
  runtime-target startup now imports the resolved standalone/family planning shapes from
  `runtime_target_planning.rs` /
  `family_runtime_planning.rs` directly; this keeps the production shared-bootstrap + shared-stream
  path pointed at the narrower owners instead of drifting back toward the compatibility facade in
  `family_runtime.rs`
- standalone/family bootstrap lifecycle ownership is narrower now too:
  bootstrap-run decisions (`Skip` / `AlreadyCompleted` / `Run`), restart-safe
  `last_processed_block + 1` resolution, and the overflow/consistency checks around those paths
  now live in `extractor/bootstrap_lifecycle.rs`, and both
  `extractor/extractor_lifecycle.rs` and `extractor/family_lifecycle.rs` delegate to that shared
  helper instead of carrying subtly different bootstrap/resume rules
- future-family extensibility is now proven through the repo-local contract gate as well:
  `scripts/check-combined-family-extensibility.sh run` exercises the manifest-backed custom-family
  registry, shared-bootstrap planning, decoder injection, managed-startup collapse, record-
  substreams request shaping, and shared-cursor resume checks without reintroducing runner-level
  branching for the new family surface
- production registry/stream lookup paths now route through those narrower owners too:
  `FamilyRuntimeRegistry` / `FamilyRuntimeSpec` from `family_registry.rs` and
  `ResolvedSharedBootstrapExecution` from `family_bootstrap_registry.rs`, and the remaining
  production shared-stream identity lookup in `substreams/stream.rs` now calls
  `family_registry::default_family_runtime_registry()` directly; this pushes another slice of
  production wiring off the legacy `family_runtime.rs` compatibility surface
- family substreams-param merge ownership is narrower now too:
  `merge_substreams_params(...)` and `merged_family_substreams_params(...)` now live in
  `extractor/family_runtime_planning.rs` beside the resolved family execution assembly that uses
  them, while `family_runtime.rs` keeps only crate-local compatibility re-exports for older call
  sites and test helpers; this removes another planning concern from the legacy aggregation module
- family execution test-helper ownership is narrower now too:
  `resolved_family_execution_config_from_extractor_configs_for_tests(...)` and its private
  single-family inference/detection helpers now live in `extractor/family_runtime_planning.rs`
  instead of `family_runtime.rs`; runner test support and lifecycle tests now import that helper
  from the planning module directly, so the legacy facade keeps less bespoke planning test logic
  and more purely compatibility-oriented re-exports
- more runner/config test wiring now bypasses the legacy compatibility facade:
  `runner/test/support.rs`, `runner_family_lifecycle_tests.rs`,
  `runner_family_planning_tests.rs`, `runner_family_runtime_metadata_tests.rs`,
  `config.rs` test imports, `standalone_managed_startup.rs` test imports, and the `runner.rs`
  test module import block now point at `family_registry.rs`, `family_runtime_metadata.rs`,
  `family_runtime_planning.rs`, `family_bootstrap_registry.rs`,
  `managed_stream_startup.rs`, and `runtime_target_planning.rs` directly; this removes another
  cohesive slice of test/support dependency traffic from `family_runtime.rs` and leaves it closer
  to a true compatibility facade
- metadata-owner tests have started moving off the legacy facade too:
  a first batch of registry metadata tests now lives in `extractor/family_runtime_metadata.rs`
  instead of `family_runtime.rs`, covering family-name lookup, registered protocol defaults,
  shared runtime metadata/stream identity, family runtime config validation/defaulting, and
  normalized shared route protocol filters next to the owner methods themselves; `family_runtime.rs`
  correspondingly sheds another chunk of mixed test ownership while staying behaviorally
  equivalent under targeted regression coverage
- bootstrap-owner tests have now started moving off the legacy facade as well:
  shared bootstrap family-name inference, shared bootstrap plan construction, family-member
  default validation, per-member bootstrap strategy/param parsing, execution lookup, and
  partial-family shared-bootstrap rejection coverage now live in
  `extractor/family_bootstrap_registry.rs` beside the owner methods; `family_runtime.rs`
  correspondingly drops another bootstrap-specific slice of mixed test ownership while targeted
  registry regressions keep the compatibility facade behavior pinned
- that bootstrap/registry migration now covers registry-global validation too:
  duplicate member protocol-system rejection and duplicate normalized shared-route alias
  rejection now live with `FamilyRuntimeRegistry::validate()` in
  `extractor/family_bootstrap_registry.rs`, instead of leaving registry-consistency behavior
  behind the compatibility facade
- planning-owner tests have now started moving off the legacy facade too:
  shared-family detection without explicit opt-in, explicit-runtime mismatch rejection,
  standalone preservation, family build-plan assembly, resolved family/runtime-target planning,
  detected stream identity, and explicit protocol-system selection coverage now lives in
  `extractor/family_runtime_planning.rs` next to the planning code paths themselves; this removes
  another planning-specific block of mixed ownership from `family_runtime.rs` and makes the
  remaining facade tests more clearly about compatibility wiring than core family planning
- that planning-owner migration now also covers execution-config resolution itself:
  the production-vs-test-helper execution-config parity check, precomputed shared execution
  settings, aligned start-block enforcement, shared-bootstrap consistency checks, aligned
  `stop_block` validation, merged `substreams_params` conflict rejection, and missing
  `protocol_types` rejection now live in `extractor/family_runtime_planning.rs` with the
  corresponding planning logic instead of staying behind the compatibility facade
- planning ownership now also includes the detected-family metadata assembly path:
  the registry-backed `detected_family_runtime(...)` materialization check now lives in
  `extractor/family_runtime_planning.rs`, keeping family detection/materialization behavior
  beside the planning code that consumes it
- metadata-owner coverage has widened further too:
  family-scoped member lookup and custom auxiliary-decoder lookup now live in
  `extractor/family_runtime_metadata.rs`, so the compatibility facade no longer owns those
  registry-backed metadata behaviors either
- runtime-target-owner tests have now started moving off the legacy facade as well:
  shared-family and standalone substreams execution request derivation, start/stop/params
  overrides, selector-based target lookup, unique-target selection, and available-target error
  surface coverage now lives in `extractor/runtime_target_planning.rs` beside the
  `ResolvedRuntimeTarget*` APIs; this removes another large behavior-specific slice from
  `family_runtime.rs` and leaves the facade incrementally closer to a compatibility-only layer
- that runtime-target migration now covers the initialized-account and protocol-projection
  ownership slice too: family-member initialized-account request derivation, per-block request
  coalescing, wrapper-level coalescing across resolved targets, and the explicit
  `protocol_systems`/`dci_protocol_systems` projection coverage now live in
  `extractor/runtime_target_planning.rs` with the `ResolvedRuntimeTarget*` types themselves,
  which removes another cross-cutting utility block from `family_runtime.rs` and leaves the
  facade with less direct behavioral ownership
- `family_runtime.rs` is now down to a single integration-style custom-registry/planning test plus
  the small helper surface that feeds it, which is the narrowest state the facade has reached so
  far in phase 3 and makes the remaining ownership boundary much easier to reason about
- that final integration-style facade test has now moved into
  `extractor/family_runtime_planning.rs` as well: `family_runtime.rs` no longer carries its own
  `#[cfg(test)]` module, and the custom-registry/future-family coverage now lives beside the
  planning entrypoints it actually exercises (`detect/build/resolved/targets` with a custom
  registry), leaving the facade as a pure re-export/compatibility surface
- current live combined-family bottleneck is downstream, not shared-stream orchestration:
  the latest `quote_settles_within_encoded_bounds_at_quote_block_for_combined_uniswap_family`
  run showed Tycho delivering initial snapshots and delta subscriptions for both family members
  quickly, while Fynd spent about `101s` computing the first block's derived data for roughly
  `7100` components before the route/settlement flow could advance; this means the present live
  gate risk is first-block Fynd warmup cost under the larger combined-family universe, not
  failure to bring up the shared Uniswap family stream itself
- the live gate now reflects that downstream warmup split more explicitly:
  combined-family route validation still defaults to `quote_ready`, but settlement validation now
  defaults back to `strict` readiness so same-block dry-run checks do not start before Fynd has
  finished the initial derived-data build for the larger shared-family universe
- shared durability provenance is now explicit in the family lifecycle too:
  `ExtractorProgressSnapshot` now carries persisted cursor/bootstrap scope metadata, real
  `ProtocolExtractor` instances distinguish shared-family durability reads from legacy
  extractor-local fallback reads, and `family_lifecycle.rs` rejects using legacy fallback state
  to skip shared bootstrap or resume the shared family stream; this closes a remaining phase-3
  gap where the combined-family runtime could still inherit old per-extractor durability state
  while appearing to run under one shared bootstrap / one shared stream model
