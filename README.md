# LOG DEMULTIPLEXER

An R&D project inspired by [Vector](https://vector.dev) — accepts log batches over UDP and fans them out to multiple destinations (Elasticsearch, Redis). Built to explore efficient log routing patterns in Rust.

> **THIS PROJECT IS NOT PRODUCTION-READY AND DOES NOT WARRANT CONTRIBUTION EFFORT GIVEN THAT [vectordev](https://vector.dev) EXISTS**

---

## What It Does

- Receives log entries over UDP (string or JSON)
- Parses and batches them, then fans out to multiple consumers concurrently
- Consumers and their settings are **dynamically configurable** via a watched config file (hot-reload, no restart needed)
- Optional **write-ahead persistence** per consumer — undelivered batches survive restarts

---

## Architecture Overview

```text

UDP Socket(s)

│

▼

UdpListener (multi-worker receivers)

│

▼

Parser (batching + parallel parse via rayon)

│

▼

Demultiplexer (fans out to N consumers)

│

├──▶ ConsumerRuntime [Redis]

├──▶ ConsumerRuntime [Elasticsearch]

└──▶ … more consumers maybe in future
```

## Configurable At Runtime

- Buffer sizes and pre-allocated slot counts
- Consumer definitions (type, endpoint, limits)
- Backpressure and drop policy per consumer
- Whether a consumer persists undelivered batches to disk

## Design Choices (Performance & Memory)

### No Redundant Copies

Parsed batches are wrapped in Arc<ParsedBatch> once. Every consumer receives a clone of the pointer — not the data. Fanout to N consumers costs N pointer increments, not N data copies.

### Pre-allocated Ring Buffers

Both the in-memory queue and the persistence layer use fixed-size Box<[T]> ring buffers allocated once at startup. There is no heap growth, no reallocation, and no Vec shifting on removal.

### Write-Ahead Persistence

When persistence is enabled for a consumer, a batch is written to disk ({logical_idx}.bin) before it enters the in-memory queue. On delivery confirmation the file is deleted. If the process crashes mid-flight, the file remains and is replayed on next boot. Parallel loading at startup uses rayon.

### Eviction Without Allocation

When a ring buffer slot is full and a new entry arrives, the oldest entry’s file is deleted and its slot is overwritten in-place. No allocations, no resizing.

### Parallel Parsing

When batch size crosses a threshold, rayon is used to parse entries in parallel across available CPU threads. Below the threshold it stays single-threaded to avoid scheduling overhead.

### Minimal Lock Contention

Each consumer has its own isolated runtime, channel, and persistence buffer. There is no shared mutable state across consumers. Locks are scoped tightly and dropped before any I/O.

## Status

This is an R&D and learning project. The goal was to explore how far you can push a log fan-out system in Rust while keeping allocations predictable and copies minimal — not to replace Vector.
