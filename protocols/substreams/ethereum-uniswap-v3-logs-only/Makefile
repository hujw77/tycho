build:
	cargo build --target wasm32-unknown-unknown --release

pack: build
	substreams pack ethereum-uniswap-v3.yaml -o uniswap-v3-v0.1.2.spkg
