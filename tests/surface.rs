//! Integration-test placeholder.
//!
//! `stryke-scylla` is a `cdylib`-only crate (no `rlib`), so a `tests/`
//! integration test cannot link against its `extern "C"` exports. The real
//! coverage is:
//!
//!   * `src/lib.rs` `#[cfg(test)] mod tests` — unit tests for the pure logic
//!     (CQL string escaping, identifier quoting, contact-point normalization,
//!     `CqlValue`→JSON). These run on `cargo test`.
//!   * `t/test_stryke_scylla_surface.stk` — pins that every `Scylla::*` wrapper
//!     resolves, with no cluster required.
//!   * `t/test_scylla.stk` — end-to-end keyspace/table/insert/query against a
//!     live ScyllaDB/Cassandra at `$SCYLLA_NODES`, short-circuited when none
//!     answers.

#[test]
fn cdylib_crate_compiles() {
    // Reaching this test means every `extern "C"` `scylla__*` export
    // type-checked and linked into the test harness's dependency graph.
}
