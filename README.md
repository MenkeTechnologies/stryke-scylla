```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                  [ s c y l l a ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-scylla/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-scylla/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[SCYLLADB / CASSANDRA CLIENT FOR STRYKE // CQL QUERY + DDL + SCHEMA]`

> *"Wide-column at scale, one stryke pipe at a time."*

ScyllaDB / Apache Cassandra client for stryke. Run CQL queries, manage
keyspaces and tables, and introspect the schema against any ScyllaDB or
Cassandra cluster over the native CQL binary protocol. Opt-in package tier.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-clickhouse`](https://github.com/MenkeTechnologies/stryke-clickhouse) · [`stryke-mongo`](https://github.com/MenkeTechnologies/stryke-mongo) · [`stryke-redis`](https://github.com/MenkeTechnologies/stryke-redis)

### [`Read the Docs`](https://menketechnologies.github.io/stryke-scylla/) &middot; [`Engineering Report`](https://menketechnologies.github.io/stryke-scylla/report.html)

---

## Table of Contents

- [\[0x00\] Install](#0x00-install)
- [\[0x01\] Quick start](#0x01-quick-start)
- [\[0x02\] Connecting](#0x02-connecting)
- [\[0x03\] Architecture](#0x03-architecture)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] Build & test](#0x05-build--test)
- [\[0x06\] License](#0x06-license)

---

## \[0x00\] Install

```sh
s add github.com/MenkeTechnologies/stryke-scylla
```

On first `use Scylla`, stryke dlopens the cdylib in-process and registers every
`scylla__*` export.

---

## \[0x01\] Quick start

```perl
use Scylla

var %conn
$conn{node}     = "127.0.0.1:9042"
$conn{keyspace} = "app"

Scylla::create_keyspace("app", %conn)
Scylla::create_table(
    "app.users",
    columns     => "id uuid, name text, age int",
    primary_key => "id",
    %conn,
)

Scylla::execute("INSERT INTO app.users (id, name, age) VALUES (uuid(), 'ada', 36)", %conn)

p Scylla::count("app.users", %conn)
val @rows = Scylla::query("SELECT name, age FROM app.users", %conn)
```

---

## \[0x02\] Connecting

Connection params come from the `%conn` opts hash on every call (or
`$SCYLLA_NODES`, comma-separated, when no node is given):

| Key        | Default          | Notes                                              |
| ---------- | ---------------- | -------------------------------------------------- |
| `node`     | `127.0.0.1:9042` | One contact point (`host` or `host:port`)          |
| `host`     | —                | Alias for `node`; combine with `port`              |
| `port`     | `9042`           | Default CQL port                                   |
| `nodes`    | —                | Array of contact points (overrides `node`/`host`)  |
| `username` | —                | CQL auth user (PasswordAuthenticator)              |
| `password` | —                | CQL auth password                                  |
| `keyspace` | —                | `USE`d on the session after connect                |

A `Session` is cached per `(nodes, auth, keyspace)` for the life of the stryke
process; the driver maintains its own per-node connection pool.

---

## \[0x03\] Architecture

- **Transport** — the CQL binary protocol via ScyllaDB's official pure-Rust
  driver (the [`scylla`](https://docs.rs/scylla) crate), which also speaks
  Apache Cassandra.
- **Sync over async** — the driver is async; the cdylib owns ONE embedded tokio
  runtime and `block_on`s each call, so the stryke-facing API stays synchronous.
- **Rows as JSON** — each `CqlValue` maps to its natural JSON type (ints, text,
  bool, lists/sets/maps), with a debug-string fallback for exotic types; rows
  come back keyed by column name.
- **Unparameterized CQL** — interpolate untrusted values through `Scylla::escape`
  (CQL doubles the single quote). The pure helpers are unit-tested in-crate, so
  they validate in CI without a cluster.

---

## \[0x04\] API reference

| Group         | Functions                                                                     |
| ------------- | ----------------------------------------------------------------------------- |
| Liveness      | `version`, `ping`, `server_version`, `cluster_name`, `peers`                    |
| Query         | `query`, `query_row`, `query_value`, `execute`, `batch`, `raw`                  |
| Introspection | `keyspaces`, `tables`, `columns`, `count`, `indexes`, `views`, `types`, `partition_keys`, `clustering_keys` |
| DDL           | `create_keyspace`, `drop_keyspace`, `create_table`, `drop_table`, `create_index`, `drop_index`, `truncate` |
| Pure helpers  | `escape`, `quote_literal`, `quote_ident`, `valid_identifier`, `format_value`, `format_in_list`, `contact_points` |

```perl
# escape untrusted input before interpolating
val $name = Scylla::escape($user_input)
val @hits = Scylla::query("SELECT * FROM app.users WHERE name = '$name' ALLOW FILTERING", %conn)
```

---

## \[0x05\] Build & test

```sh
make debug       # cargo build
make test        # cargo test, then `s test t/` (needs $SCYLLA_NODES or 127.0.0.1:9042)
make install     # s pkg install -g .
```

`cargo test` runs the in-crate unit tests (CQL escaping, identifier quoting,
contact-point normalization, `CqlValue`→JSON) with no cluster required. The
`t/test_stryke_scylla_surface.stk` pins the wrapper surface; `t/test_scylla.stk`
runs end-to-end keyspace/table/insert/query against a live cluster and
short-circuits when none answers.

---

## \[0x06\] License

MIT &middot; MenkeTechnologies
