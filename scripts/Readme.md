# Getting started

## Migrating a PR from a related repo

When a PR is open in a related repository (`tycho-protocol-sdk`, `tycho-simulation`,
`tycho-execution`) and the work needs to land in this monorepo, use `migrate-pr.sh`.

### Prerequisites

1. Clone (or have a local checkout of) the source repository.
2. **Resolve any merge conflicts first**: rebase the source branch onto the source repo's `main`
   before migrating. Unresolved conflicts produce broken patches that fail during `git am`.

   ```bash
   cd ../tycho-protocol-sdk
   git fetch origin
   git rebase origin/main <branch-name>
   # resolve conflicts, then git rebase --continue
   ```

### Usage

Path mappings are **looked up automatically** from the source repo name. Just pass the repo
path and branch — no mapping arguments needed for known repos:

```bash
./scripts/migrate-pr.sh <source-repo-path> <branch-name>
```

| Source repo | Mappings applied automatically |
|---|---|
| `tycho-protocol-sdk` | `substreams→protocols/substreams`, `evm→protocols/adapter-integration/evm`, `protocol-testing→protocols/testing` |
| `tycho-simulation` | everything → `crates/tycho-simulation/` |
| `tycho-execution` | everything → `crates/tycho-execution/` |

```bash
# tycho-protocol-sdk PR — no extra args needed:
./scripts/migrate-pr.sh ../tycho-protocol-sdk ah/ENG-5053/fluid-indexing

# tycho-simulation PR:
./scripts/migrate-pr.sh ../tycho-simulation ah/my-feature
```

### Custom / extra mappings

Pass additional `src:dst` arguments to extend the default mappings (e.g. to bring over a
CI file change, or to migrate a new repo not yet in the table):

```bash
./scripts/migrate-pr.sh ../tycho-protocol-sdk ah/my-feature \
  ".github/workflows:protocols/ci"
```

For a repo not in the table, pass all mappings explicitly and add the repo to the table
in `migrate-pr.sh` for future use.

### Known manual steps after applying

The script automates path rewriting and strips common problem cases, but some things
need manual handling:

**Cargo.lock**: always stripped and must be regenerated after migration:
```bash
cargo check --workspace
git add -p  # stage only the Cargo.lock changes you want
```

**Cargo.toml / source file context conflicts**: when `-C0` can't apply a patch, the script
retries with `--reject`. Git applies all hunks it can and writes `<file>.rej` files for
the rest. Use `wiggle` to apply them — it uses word-level diffing and inserts
`<<<<<<<`/`=======`/`>>>>>>>` conflict markers for anything it can't resolve automatically:

```bash
brew install wiggle  # one-time setup

# Apply all .rej files; conflict markers appear in-place for anything unresolved
find . -name '*.rej' | while read -r rej; do
  target="${rej%.rej}"
  wiggle --merge "$target" "$rej" && rm "$rej"
done

# Resolve any remaining conflict markers in your editor, then:
git add <resolved-files>
git am --continue
```

**`include_str!()` and path literals**: the script rewrites path segments on added content
lines alongside the diff headers. A reference like `../../evm/test/executors/X.json` in
`protocol-testing/src/` is automatically rewritten to `../../adapter-integration/evm/test/executors/X.json`
so it resolves correctly from `protocols/testing/src/`.

### After migration

1. Run `cargo check --workspace` to regenerate `Cargo.lock`.
2. Push the branch and open a PR against this repo.
3. Close the original PR with a comment linking to the new PR.

---



## Compare scripts

All comparison scripts rely on using an archive node. You will need to set it using the 
`ETH_RPC_URL` env var.


### UniswapV2 & Balancer

These scripts are made to verify our data against a trusted source.

To run them you will first need to get some data from Tycho RPC. Use the state endpoints to 
get the state of the protocol you want to check and store the result in a json file with this 
name format: `{protocol}_{block_number}.json`. For example `uniswap_v2_10000.json`

Then and run it with the following command:
```bash
python compare-uniswap-v2.py <block_number>
```

Note, the script uses web3. If you have not got it installed already, you will need to do so:
```bash
pip install web3
```


### UniswapV3

You'll need the requests library installed, then pass block and pool addresses to compare:

```bash
python scripts/compare-uniswap-v3-the-graph.py \
    19510400 \
    0x1385fc1fe0418ea0b4fcf7adc61fc7535ab7f80d \
    0x6b6c7beadce465f8f2ada88903bdbbb170fa1f10
```

## Combined Uniswap fixture capture

Use `scripts/combined-family-history-slice-fixture.sh` to drive the checked-in combined
Uniswap V2/V3 history-slice fixture workflow.

Check whether the local environment is ready for the real capture workflow:

```bash
scripts/combined-family-history-slice-fixture.sh doctor
```

Fail fast when the required external inputs are missing:

```bash
scripts/combined-family-history-slice-fixture.sh doctor --strict
```

Preflight the resolved shared-family request without opening a Substreams session:

```bash
scripts/combined-family-history-slice-fixture.sh preflight
```

Print the exact live capture command before running it:

```bash
scripts/combined-family-history-slice-fixture.sh command
```

Capture the real fixture into
`crates/tycho-indexer/tests/fixtures/combined_family_real_history_slice.json`:

```bash
TYCHO_RECORD_ENDPOINT=https://mainnet.eth.streamingfast.io \
TYCHO_RECORD_RPC_URL=https://rpc.mevblocker.io \
SUBSTREAMS_API_TOKEN=... \
scripts/combined-family-history-slice-fixture.sh record
```

Optional overrides:

- `TYCHO_COMBINED_FIXTURE_START_BLOCK`
- `TYCHO_COMBINED_FIXTURE_STOP_BLOCK`
- `TYCHO_COMBINED_FIXTURE_OUTPUT`
- `TYCHO_COMBINED_FIXTURE_CONFIG`

## Combined Uniswap live Fynd E2E

Use `scripts/check-combined-family-fynd-live-e2e.sh` to standardize the live combined-family
Fynd route-return and quote-settlement checks against a local Tycho RPC plus a live Ethereum RPC.

Check whether the sibling `fynd` repository is present and whether the local Tycho endpoint is
reachable:

```bash
scripts/check-combined-family-fynd-live-e2e.sh doctor
```

Fail fast when the local Tycho endpoint is not ready:

```bash
scripts/check-combined-family-fynd-live-e2e.sh doctor --strict
```

Print the exact combined-family route + settlement command sequence before running it:

```bash
scripts/check-combined-family-fynd-live-e2e.sh command all
```

Run only the combined-family route-return ignored test:

```bash
scripts/check-combined-family-fynd-live-e2e.sh run-route
```

Run only the combined-family quote-settlement ignored test:

```bash
scripts/check-combined-family-fynd-live-e2e.sh run-settlement
```

Run both combined-family ignored tests in sequence:

```bash
scripts/check-combined-family-fynd-live-e2e.sh run-all
```

Optional overrides:

- `FYND_REPO_ROOT`
- `FYND_E2E_TYCHO_URL`
- `FYND_E2E_RPC_URL`
- `FYND_E2E_RUST_LOG`
- `TYCHO_STREAM_WS_BUFFER_SIZE`
- `TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE`

## Combined Uniswap indexer startup

Use `scripts/run-combined-family-indexer.sh` to standardize the local combined-family Tycho
indexer startup command.

Check whether the local startup environment is ready:

```bash
scripts/run-combined-family-indexer.sh doctor
```

Fail fast when required inputs such as `SUBSTREAMS_API_TOKEN` or the local Postgres endpoint are
missing:

```bash
scripts/run-combined-family-indexer.sh doctor --strict
```

Print the exact startup command:

```bash
scripts/run-combined-family-indexer.sh command
```

Start the combined-family indexer directly:

```bash
scripts/run-combined-family-indexer.sh run
```

Optional overrides:

- `AUTH_API_KEY`
- `SUBSTREAMS_API_TOKEN`
- `TYCHO_INDEXER_ENDPOINT`
- `TYCHO_INDEXER_DATABASE_URL`
- `TYCHO_INDEXER_RPC_URL`
- `TYCHO_INDEXER_EXTRACTORS_CONFIG`
- `TYCHO_INDEXER_RUST_LOG`

Notes:

- `AUTH_API_KEY` defaults to `dummy` for local operator runs, matching the current indexer
  startup requirement without forcing a separate secret-management step.
- the script prints and runs `export SUBSTREAMS_API_TOKEN=...` before
  `--api_token "$SUBSTREAMS_API_TOKEN"` so the combined-family startup path does not regress to
  the shell-expansion bug where an inline env assignment leaves `--api_token` empty

## Combined Uniswap validation surface

Use `scripts/check-combined-family.sh` as the top-level Phase 3 validation entrypoint. It
composes:

- the repo-local DB-backed combined-family regression gate
- the live combined-family Fynd E2E gate

Check aggregate readiness:

```bash
scripts/check-combined-family.sh doctor
```

Fail fast when either the local DB-backed gate or the live Tycho/Fynd gate is not ready:

```bash
scripts/check-combined-family.sh doctor --strict
```

Print the exact repo-local regression command:

```bash
scripts/check-combined-family.sh command repo
```

Print the exact live Fynd E2E command sequence:

```bash
scripts/check-combined-family.sh command live
```

Print the full Phase 3 validation command sequence:

```bash
scripts/check-combined-family.sh command all
```

Run only the repo-local DB-backed regression gate:

```bash
scripts/check-combined-family.sh run-repo
```

Run only the live Fynd E2E gate:

```bash
scripts/check-combined-family.sh run-live
```

Run the full combined-family validation sequence:

```bash
scripts/check-combined-family.sh run-all
```
