.PHONY: build test bench clean

build:
	cargo build

test:
	cargo test --lib --bins -- --test-threads=1

bench:
	cargo bench --bench udp_bench

clean:
	cargo clean
