build:
	cargo build --target wasm32-unknown-unknown --release

pack: build
	substreams pack ethereum-uniswap-v2-v3.yaml -o ethereum-uniswap-v2-v3-combined-v0.1.0.spkg

pack-v2-created-only: build
	substreams pack ethereum-uniswap-v2-v3-v2-created-only.yaml -o ethereum-uniswap-v2-v3-v2-created-only-v0.1.0.spkg

pack-v2-created-store: build
	substreams pack ethereum-uniswap-v2-v3-v2-created-store.yaml -o ethereum-uniswap-v2-v3-v2-created-store-v0.1.0.spkg

pack-v3-created-only: build
	substreams pack ethereum-uniswap-v2-v3-v3-created-only.yaml -o ethereum-uniswap-v2-v3-v3-created-only-v0.1.0.spkg

pack-v3-created-store: build
	substreams pack ethereum-uniswap-v2-v3-v3-created-store.yaml -o ethereum-uniswap-v2-v3-v3-created-store-v0.1.0.spkg
