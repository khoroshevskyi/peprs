wasm:
	wasm-pack build --target web --release peprs-wasm
	jq '.name = "@pepkit/peprs" | .repository = {"type": "git", "url": "https://github.com/khoroshevskyi/peprs"}' peprs-wasm/pkg/package.json > tmp.json && mv tmp.json peprs-wasm/pkg/package.json

test:
	cargo test --all --workspace -- --nocapture

fmt:
	cargo fmt --all -- --check