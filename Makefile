.PHONY: build test bench clean

build:
	cargo build

test:
	cargo test --lib --bins -- --test-threads=1

bench_udp:
	cargo bench --bench udp_bench
bench_parser:
	cargo bench --bench parser_bench

clean:
	cargo clean
