build:
	cargo build --target wasm32-unknown-unknown --release

pack: build
	substreams pack ethereum-uniswap-v2-v3.yaml -o ethereum-uniswap-v2-v3-combined-v0.1.0.spkg
