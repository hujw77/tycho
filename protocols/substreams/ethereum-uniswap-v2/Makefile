build:
	cargo build --target wasm32-unknown-unknown --release

pack: build
	substreams pack ethereum-uniswap-v2.yaml -o ethereum-uniswap-v2-v0.3.2.spkg
