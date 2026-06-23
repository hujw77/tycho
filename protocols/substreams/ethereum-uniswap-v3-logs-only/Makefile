build:
	cargo build --target wasm32-unknown-unknown --release

pack: build
	substreams pack ethereum-uniswap-v3.yaml -o ethereum-uniswap-v3-logs-only-v0.1.2.spkg
