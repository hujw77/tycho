# Uniswap V2/V3 Shared Bootstrap And Combined Substream Plan

## Status

- Phase 1: complete
- Phase 2: complete
- Phase 3: in progress

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
- runtime architecture is still unchanged: V2 and V3 remain separate extractors, sessions,
  cursors, and recovery paths

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
4. Preserve the option to keep V2 and V3 failure domains separate until a later phase.

## Non-Goals

1. Changing Tycho RPC response formats.
2. Merging `protocol_system` identities exposed to clients.
3. Rewriting Fynd integration logic.
4. Unifying V2/V3 simulation or decoding logic.

## Recommendation

Use a phased rollout:

1. Shared bootstrap configuration and parameter expansion
2. Shared extractor manifest generation or composition
3. Optional combined Substreams package only after the first two phases are stable

This ordering captures most of the operational benefit while keeping the highest-risk
change, combined runtime execution, for last.

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

## Phase 3: Optional Combined Substream

Status: started

### What changes

Build a new package that consumes one chain block stream and emits protocol-specific
outputs for both V2 and V3.

Conceptually:

```text
source block
  -> shared log prefilter
  -> V2 branch
  -> V3 branch
  -> protocol-specific BlockChanges outputs
```

The important constraint is that Tycho should still expose two logical extractors:

- `uniswap_v2`
- `uniswap_v3`

Even if the upstream Substreams package is combined, the indexer-facing identities
should remain stable.

### Viable models

#### Model A: One combined package, two output modules

- one `.spkg`
- one V2 output module
- one V3 output module
- indexer still subscribes twice, but to different modules in the same package

This reduces package sprawl and aligns bootstrap/config management, but still keeps
separate cursors and extractor state inside Tycho.

#### Model B: One combined package, one unified output module

- one `.spkg`
- one module emits a tagged stream containing both V2 and V3 changes
- Tycho splits the stream downstream

This maximizes shared work upstream, but it is higher risk:

- larger failure domain
- more complex revert semantics
- more indexer-side demux logic

### Recommendation

Prefer Model A first if combined substream work is pursued at all.

It offers:

- shared package build and bootstrap logic
- partial reduction in duplication
- stable extractor identities
- lower blast radius than a single unified mixed stream

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

The first engineering slice after the spike has now landed:

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
- tycho-indexer now has an optional combined-runtime entrypoint:
  `crates/tycho-indexer/extractors.uniswap_v2_v3.combined.yaml`

This keeps the current deployment model stable while clearing the main technical blocker that
prevented a combined package from reusing existing logic safely.

### Recommended Implementation Slice

The next concrete Phase 3 slice should be:

1. Wire the combined package into indexer config as an optional runtime path.
2. Validate that the combined package preserves standalone behavior for:
   - pool creation
   - event filtering
   - balances
   - ticks
   - liquidity
   - protocol changes
3. Decide whether the combined package should become the default source for the V2+V3 extractor
   entrypoint or remain an opt-in deployment mode.

That slice is now partially complete:

1. the combined package has been wired into indexer config as an optional runtime path
2. config-level validation is in place for the combined entrypoint
3. runtime validation is still in progress, especially around pool scoping and post-bootstrap
   updates

### Next Slice: Dynamic Factory Pool Admission

The next major gap is that the current combined/bootstrap-oriented path is still biased toward
the statically configured bootstrap pool set.

That is sufficient for controlled validation, but it is not sufficient for production operation
if the extractor is expected to keep discovering and admitting newly created pools from the
factory over time.

The next follow-up slice should therefore add dynamic factory pool admission while preserving
the bootstrap guarantees already established for known pools.

#### Scope

1. keep bootstrap route filtering as the initial seed set for V2 and V3
2. continue listening to factory `PoolCreated` or equivalent creation events after bootstrap
3. materialize newly discovered pools into Tycho state automatically
4. ensure downstream event modules begin accepting updates for those newly admitted pools
   without requiring a manual bootstrap config change
5. preserve protocol-aware filtering so V2 and V3 branches do not ingest each other's pools

#### Design Constraints

1. bootstrap configuration should define the initial synchronization scope, not act as a hard
   forever-allowlist unless explicitly configured that way
2. dynamic admission must not regress the recent fix that prevents runtime processing of foreign
   or not-yet-known pools
3. newly discovered pools must become visible through the same Tycho RPC surfaces:
   `protocol_components`, `protocol_state`, and protocol component state snapshots
4. cursor resume and reorg handling must remain extractor-local even if pool discovery is shared

#### Acceptance Criteria

1. starting from the shared bootstrap seed set, the extractor later ingests a newly created V2
   pool without editing bootstrap YAML
2. starting from the shared bootstrap seed set, the extractor later ingests a newly created V3
   pool without editing bootstrap YAML
3. newly admitted pools receive follow-up state updates, not just creation records
4. Tycho RPC exposes the new pools through `protocol_components`
5. Fynd can route through a dynamically admitted pool once it becomes relevant

#### Recommended Rollout

1. first support dynamic admission in standalone V2 and V3 paths
2. then preserve the same behavior in the combined package path
3. finally decide whether combined mode can become the default runtime for shared Uniswap family
   deployments

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

- cursor resume works independently for both logical extractors
- reorg handling preserves extractor-local revert semantics
- V2 branch failure does not corrupt V3 persisted state, and vice versa
- factory-discovered pools are admitted after bootstrap and continue receiving state updates
- combined mode does not treat bootstrap pools as a permanent allowlist unless configured to do so

## Recommended Next Slice

The next implementation slice should be:

1. complete runtime validation for the optional combined entrypoint
2. add dynamic factory pool admission on top of the shared bootstrap seed model
3. add regression coverage that proves newly discovered V2/V3 pools become queryable through
   Tycho RPC and continue receiving follow-up updates

This keeps the bootstrap unification work intact while addressing the main remaining production
gap: factory-discovered pools must join the indexed universe automatically.
