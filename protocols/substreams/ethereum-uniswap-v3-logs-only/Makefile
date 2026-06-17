sync-bootstrap-manifest:
	./scripts/sync-bootstrap-manifest.sh

build:
	cargo build --target wasm32-unknown-unknown --release

pack: build
	substreams pack ethereum-uniswap-v3.yaml -o uniswap-v3-v0.1.2.spkg

pack-bootstrap: sync-bootstrap-manifest build
	substreams pack uniswap-v3-bootstrap.yaml -o uniswap-v3-bootstrap-v0.1.0.spkg
