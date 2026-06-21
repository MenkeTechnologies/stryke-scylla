//! stryke-scylla — ScyllaDB / Cassandra cdylib loaded in-process by stryke via
//! dlopen.
//!
//! Each `#[no_mangle] extern "C" fn scylla__*` is a JSON-string-in /
//! JSON-string-out wrapper. stryke's FFI bridge resolves these symbols at first
//! `use Scylla`, passes a JSON-encoded args dict per call, and copies the
//! returned JSON into a stryke string; `stryke_free_cstring` frees it.
//!
//! Transport is the CQL binary protocol via ScyllaDB's official pure-Rust
//! driver (`scylla` crate), which is async. The cdylib owns ONE embedded tokio
//! runtime and `block_on`s each driver call, so the FFI surface stays
//! synchronous. A `Session` is cached per `(nodes, auth, keyspace)` for the
//! life of the stryke process.
//!
//! Queries are unparameterized CQL — interpolate untrusted values through the
//! pure `scylla__escape_string` helper (CQL escapes `'` by doubling it). Rows
//! come back as JSON objects keyed by column name; `CqlValue`s are mapped to
//! their natural JSON types, with a debug-string fallback for exotic types.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::value::{CqlValue, Row};
use serde_json::{json, Value};
use tokio::runtime::Runtime;

// ── embedded runtime ────────────────────────────────────────────────────────

static RT: OnceCell<Runtime> = OnceCell::new();

fn rt() -> &'static Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .expect("build tokio runtime")
    })
}

// ── session cache ───────────────────────────────────────────────────────────

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct ConnKey {
    nodes: Vec<String>,
    username: String,
    password: String,
    keyspace: String,
}

static SESSIONS: OnceCell<Mutex<HashMap<ConnKey, Arc<Session>>>> = OnceCell::new();

fn sessions() -> &'static Mutex<HashMap<ConnKey, Arc<Session>>> {
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Resolve contact points + auth + keyspace from an opts dict. Accepts `nodes`
/// (array of `host` or `host:port`), or `node`/`host` (+ `port`). Default
/// `127.0.0.1:9042`.
fn conn_from_opts(opts: &Value) -> ConnKey {
    let mut nodes: Vec<String> = Vec::new();
    if let Some(arr) = opts.get("nodes").and_then(|v| v.as_array()) {
        for n in arr {
            if let Some(s) = n.as_str() {
                nodes.push(normalize_node(s));
            }
        }
    } else if let Some(n) = opts
        .get("node")
        .or_else(|| opts.get("host"))
        .and_then(|v| v.as_str())
    {
        let port = opts.get("port").and_then(|v| v.as_i64());
        nodes.push(match port {
            Some(p) if !n.contains(':') => format!("{}:{}", n, p),
            _ => normalize_node(n),
        });
    }
    if nodes.is_empty() {
        nodes.push("127.0.0.1:9042".to_string());
    }
    ConnKey {
        nodes,
        username: opts
            .get("username")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        password: opts
            .get("password")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        keyspace: opts
            .get("keyspace")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
    }
}

/// Add the default CQL port (9042) when a contact point omits it.
fn normalize_node(s: &str) -> String {
    if s.contains(':') {
        s.to_string()
    } else {
        format!("{}:9042", s)
    }
}

/// Get or build the cached `Session` for this opts dict.
fn get_session(opts: &Value) -> Result<Arc<Session>> {
    let key = conn_from_opts(opts);
    {
        let map = sessions().lock();
        if let Some(s) = map.get(&key) {
            return Ok(Arc::clone(s));
        }
    }
    let session = rt().block_on(build_session(&key))?;
    let arc = Arc::new(session);
    sessions().lock().insert(key, Arc::clone(&arc));
    Ok(arc)
}

async fn build_session(key: &ConnKey) -> Result<Session> {
    let mut b = SessionBuilder::new().known_nodes(&key.nodes);
    if !key.username.is_empty() || !key.password.is_empty() {
        b = b.user(&key.username, &key.password);
    }
    let session = b.build().await.map_err(|e| anyhow!("connect: {}", e))?;
    if !key.keyspace.is_empty() {
        session
            .use_keyspace(&key.keyspace, false)
            .await
            .map_err(|e| anyhow!("use keyspace {}: {}", key.keyspace, e))?;
    }
    Ok(session)
}

// ── query execution ─────────────────────────────────────────────────────────

/// Run a CQL statement. SELECT-style results become an array of row objects;
/// statements with no result set (DDL/DML) return `{ "ok": true }`.
fn run_cql(opts: &Value, cql: &str) -> Result<Value> {
    let session = get_session(opts)?;
    rt().block_on(async move {
        let res = session
            .query_unpaged(cql, &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        match res.into_rows_result() {
            Ok(rows) => {
                let names: Vec<String> = rows
                    .column_specs()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect();
                let mut out = Vec::new();
                for row in rows.rows::<Row>().map_err(|e| anyhow!("{}", e))? {
                    let row = row.map_err(|e| anyhow!("{}", e))?;
                    let mut obj = serde_json::Map::new();
                    for (i, col) in row.columns.iter().enumerate() {
                        let name = names.get(i).cloned().unwrap_or_else(|| i.to_string());
                        let val = match col {
                            Some(v) => cql_to_json(v),
                            None => Value::Null,
                        };
                        obj.insert(name, val);
                    }
                    out.push(Value::Object(obj));
                }
                Ok(json!({ "rows": out }))
            }
            // not a rows result (DDL / INSERT / UPDATE / DELETE)
            Err(_) => Ok(json!({ "ok": true })),
        }
    })
}

/// Best-effort `CqlValue` → JSON. Common scalar/collection types map to their
/// natural JSON form; anything exotic falls back to its debug string so the
/// converter never fails to produce a value.
fn cql_to_json(v: &CqlValue) -> Value {
    match v {
        CqlValue::Boolean(b) => json!(b),
        CqlValue::Int(i) => json!(i),
        CqlValue::BigInt(i) => json!(i),
        CqlValue::SmallInt(i) => json!(i),
        CqlValue::TinyInt(i) => json!(i),
        CqlValue::Float(f) => json!(f),
        CqlValue::Double(f) => json!(f),
        CqlValue::Text(s) | CqlValue::Ascii(s) => json!(s),
        CqlValue::Uuid(u) => json!(u.to_string()),
        CqlValue::Empty => Value::Null,
        CqlValue::List(items) | CqlValue::Set(items) => {
            Value::Array(items.iter().map(cql_to_json).collect())
        }
        CqlValue::Map(pairs) => Value::Array(
            pairs
                .iter()
                .map(|(k, val)| json!([cql_to_json(k), cql_to_json(val)]))
                .collect(),
        ),
        other => json!(format!("{:?}", other)),
    }
}

// ── extractors ──────────────────────────────────────────────────────────────

fn str_field<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v.get(k)
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow!("missing {}", k))
}

fn opt_str<'a>(v: &'a Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(|x| x.as_str())
}

/// The `rows` array of a `run_cql` result (empty for non-row statements).
fn rows_of(result: &Value) -> Value {
    result.get("rows").cloned().unwrap_or(json!([]))
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call<F>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Result<Value>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| handler(input)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-scylla handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── version + liveness ──────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn scylla__version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
}

#[no_mangle]
pub extern "C" fn scylla__ping(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let ok = run_cql(&v, "SELECT release_version FROM system.local").is_ok();
        Ok(json!({ "value": ok }))
    })
}

#[no_mangle]
pub extern "C" fn scylla__server_version(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let r = run_cql(&v, "SELECT release_version FROM system.local")?;
        let ver = rows_of(&r)
            .get(0)
            .and_then(|row| row.get("release_version"))
            .cloned()
            .unwrap_or(Value::Null);
        Ok(json!({ "value": ver }))
    })
}

// ── query ───────────────────────────────────────────────────────────────────

/// Run a SELECT; returns the array of row objects.
#[no_mangle]
pub extern "C" fn scylla__query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cql = str_field(&v, "cql")?;
        let r = run_cql(&v, cql)?;
        Ok(json!({ "value": rows_of(&r) }))
    })
}

/// Run a SELECT; returns the first row object (or null).
#[no_mangle]
pub extern "C" fn scylla__query_row(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cql = str_field(&v, "cql")?;
        let r = run_cql(&v, cql)?;
        Ok(json!({ "value": rows_of(&r).get(0).cloned().unwrap_or(Value::Null) }))
    })
}

/// Run a SELECT; returns the first column of the first row (a scalar).
#[no_mangle]
pub extern "C" fn scylla__query_value(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cql = str_field(&v, "cql")?;
        let r = run_cql(&v, cql)?;
        let val = rows_of(&r)
            .get(0)
            .and_then(|row| row.as_object())
            .and_then(|o| o.values().next())
            .cloned()
            .unwrap_or(Value::Null);
        Ok(json!({ "value": val }))
    })
}

/// Run a statement that returns no rows (INSERT/UPDATE/DELETE/DDL).
#[no_mangle]
pub extern "C" fn scylla__execute(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cql = str_field(&v, "cql")?;
        run_cql(&v, cql)?;
        Ok(json!({ "ok": true }))
    })
}

// ── schema introspection ────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn scylla__keyspaces(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let r = run_cql(&v, "SELECT keyspace_name FROM system_schema.keyspaces")?;
        let names: Vec<Value> = rows_of(&r)
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|row| row.get("keyspace_name").cloned())
                    .collect()
            })
            .unwrap_or_default();
        Ok(json!({ "value": names }))
    })
}

#[no_mangle]
pub extern "C" fn scylla__tables(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let ks = opt_str(&v, "keyspace").ok_or_else(|| anyhow!("missing keyspace"))?;
        let cql = format!(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{}'",
            escape_string(ks)
        );
        let r = run_cql(&v, &cql)?;
        let names: Vec<Value> = rows_of(&r)
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|row| row.get("table_name").cloned())
                    .collect()
            })
            .unwrap_or_default();
        Ok(json!({ "value": names }))
    })
}

#[no_mangle]
pub extern "C" fn scylla__columns(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let ks = opt_str(&v, "keyspace").ok_or_else(|| anyhow!("missing keyspace"))?;
        let table = str_field(&v, "table")?;
        let cql = format!(
            "SELECT column_name, type, kind FROM system_schema.columns \
             WHERE keyspace_name = '{}' AND table_name = '{}'",
            escape_string(ks),
            escape_string(table)
        );
        let r = run_cql(&v, &cql)?;
        Ok(json!({ "value": rows_of(&r) }))
    })
}

#[no_mangle]
pub extern "C" fn scylla__count(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = str_field(&v, "table")?;
        let r = run_cql(&v, &format!("SELECT count(*) AS c FROM {}", table))?;
        let n = rows_of(&r)
            .get(0)
            .and_then(|row| row.get("c"))
            .cloned()
            .unwrap_or(json!(0));
        Ok(json!({ "value": n }))
    })
}

// ── DDL helpers ─────────────────────────────────────────────────────────────

/// Create a keyspace. `replication` is the CQL replication map (default
/// SimpleStrategy RF 1).
#[no_mangle]
pub extern "C" fn scylla__create_keyspace(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = str_field(&v, "name")?;
        let replication = opt_str(&v, "replication")
            .unwrap_or("{'class': 'SimpleStrategy', 'replication_factor': 1}");
        let cql = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {}",
            name, replication
        );
        run_cql(&v, &cql)?;
        Ok(json!({ "ok": true }))
    })
}

#[no_mangle]
pub extern "C" fn scylla__drop_keyspace(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = str_field(&v, "name")?;
        run_cql(&v, &format!("DROP KEYSPACE IF EXISTS {}", name))?;
        Ok(json!({ "ok": true }))
    })
}

/// Create a table. Pass a full `cql` CREATE statement, or `name` + `columns`
/// (`"col type, …"`) + `primary_key` (`"(pk)"` or `"col"`).
#[no_mangle]
pub extern "C" fn scylla__create_table(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        if let Some(cql) = opt_str(&v, "cql") {
            run_cql(&v, cql)?;
            return Ok(json!({ "ok": true }));
        }
        let name = str_field(&v, "name")?;
        let columns = str_field(&v, "columns")?;
        let pk = str_field(&v, "primary_key")?;
        let cql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY ({}))",
            name, columns, pk
        );
        run_cql(&v, &cql)?;
        Ok(json!({ "ok": true }))
    })
}

#[no_mangle]
pub extern "C" fn scylla__drop_table(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = str_field(&v, "table")?;
        run_cql(&v, &format!("DROP TABLE IF EXISTS {}", table))?;
        Ok(json!({ "ok": true }))
    })
}

#[no_mangle]
pub extern "C" fn scylla__truncate(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = str_field(&v, "table")?;
        run_cql(&v, &format!("TRUNCATE {}", table))?;
        Ok(json!({ "ok": true }))
    })
}

/// Escape hatch: run arbitrary `cql`. `rows` (default true) returns the row
/// array; pass `false` for a no-row statement.
#[no_mangle]
pub extern "C" fn scylla__raw(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cql = str_field(&v, "cql")?;
        let r = run_cql(&v, cql)?;
        if v.get("rows").and_then(|x| x.as_bool()).unwrap_or(true) {
            Ok(json!({ "value": rows_of(&r) }))
        } else {
            Ok(json!({ "ok": true }))
        }
    })
}

// ── pure helpers (no network) ───────────────────────────────────────────────

/// Escape a string for a single-quoted CQL literal — CQL doubles the single
/// quote (`'` → `''`).
#[no_mangle]
pub extern "C" fn scylla__escape_string(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = str_field(&v, "value")?;
        Ok(json!({ "value": escape_string(s) }))
    })
}

/// Double-quote a CQL identifier (`"` → `""`), forcing case sensitivity.
#[no_mangle]
pub extern "C" fn scylla__quote_identifier(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = str_field(&v, "value")?;
        Ok(json!({ "value": quote_identifier(s) }))
    })
}

/// Normalize the contact points these opts resolve to (each `host:port`).
#[no_mangle]
pub extern "C" fn scylla__contact_points(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = conn_from_opts(&v);
        Ok(json!({ "value": key.nodes }))
    })
}

/// Wrap a string as a single-quoted CQL literal.
#[no_mangle]
pub extern "C" fn scylla__quote_literal(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = str_field(&v, "value")?;
        Ok(json!({ "value": quote_literal(s) }))
    })
}

/// Format a JSON value as a CQL literal (string/number/bool/null/list).
#[no_mangle]
pub extern "C" fn scylla__format_value(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let val = v.get("value").ok_or_else(|| anyhow!("missing value"))?;
        Ok(json!({ "value": format_value(val) }))
    })
}

/// Render an array of values into a CQL `IN (...)` list.
#[no_mangle]
pub extern "C" fn scylla__format_in_list(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let vals = v
            .get("values")
            .and_then(|x| x.as_array())
            .ok_or_else(|| anyhow!("missing values array"))?;
        Ok(json!({ "value": format_in_list(vals) }))
    })
}

/// True when a string is a valid unquoted CQL identifier.
#[no_mangle]
pub extern "C" fn scylla__valid_identifier(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = str_field(&v, "value")?;
        Ok(json!({ "valid": valid_identifier(s) }))
    })
}

// ── shared pure logic (unit-tested) ─────────────────────────────────────────

fn escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

fn quote_identifier(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Wrap a string as a single-quoted CQL literal (escaping embedded quotes).
fn quote_literal(s: &str) -> String {
    format!("'{}'", escape_string(s))
}

/// Format a JSON value as a CQL literal: string→`'...'`, number→as-is,
/// bool→`true`/`false`, null→`NULL`, array→CQL list `[...]`.
fn format_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => quote_literal(s),
        Value::Array(a) => format!(
            "[{}]",
            a.iter().map(format_value).collect::<Vec<_>>().join(", ")
        ),
        Value::Object(_) => quote_literal(&v.to_string()),
    }
}

/// Render values into an `IN (...)` list. Empty → `(NULL)` (matches nothing).
fn format_in_list(vals: &[Value]) -> String {
    if vals.is_empty() {
        return "(NULL)".to_string();
    }
    format!(
        "({})",
        vals.iter().map(format_value).collect::<Vec<_>>().join(", ")
    )
}

/// A CQL identifier is safe unquoted when it matches `[A-Za-z][A-Za-z0-9_]*`.
fn valid_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

// ── unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_doubles_single_quote() {
        assert_eq!(escape_string("a'b"), "a''b");
        assert_eq!(escape_string("O'Brien"), "O''Brien");
        assert_eq!(escape_string("plain"), "plain");
    }

    #[test]
    fn quote_identifier_double_quotes() {
        assert_eq!(quote_identifier("Col"), "\"Col\"");
        assert_eq!(quote_identifier("we\"ird"), "\"we\"\"ird\"");
    }

    #[test]
    fn normalize_node_adds_default_port() {
        assert_eq!(normalize_node("10.0.0.1"), "10.0.0.1:9042");
        assert_eq!(normalize_node("10.0.0.1:9999"), "10.0.0.1:9999");
    }

    #[test]
    fn conn_defaults_to_localhost() {
        let k = conn_from_opts(&json!({}));
        assert_eq!(k.nodes, vec!["127.0.0.1:9042".to_string()]);
        assert_eq!(k.keyspace, "");
    }

    #[test]
    fn conn_node_with_explicit_port() {
        let k = conn_from_opts(&json!({"host": "db", "port": 9042, "keyspace": "app"}));
        assert_eq!(k.nodes, vec!["db:9042".to_string()]);
        assert_eq!(k.keyspace, "app");
    }

    #[test]
    fn conn_nodes_array_normalizes_ports() {
        let k = conn_from_opts(&json!({"nodes": ["a", "b:9100"]}));
        assert_eq!(k.nodes, vec!["a:9042".to_string(), "b:9100".to_string()]);
    }

    #[test]
    fn cql_scalar_to_json() {
        assert_eq!(cql_to_json(&CqlValue::Int(7)), json!(7));
        assert_eq!(cql_to_json(&CqlValue::Boolean(true)), json!(true));
        assert_eq!(cql_to_json(&CqlValue::Text("hi".into())), json!("hi"));
        assert_eq!(
            cql_to_json(&CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)])),
            json!([1, 2])
        );
    }

    #[test]
    fn quote_literal_wraps_and_escapes() {
        assert_eq!(quote_literal("O'Brien"), "'O''Brien'");
        assert_eq!(quote_literal("plain"), "'plain'");
    }

    #[test]
    fn format_value_by_type() {
        assert_eq!(format_value(&json!(7)), "7");
        assert_eq!(format_value(&json!(true)), "true");
        assert_eq!(format_value(&Value::Null), "NULL");
        assert_eq!(format_value(&json!("a'b")), "'a''b'");
        assert_eq!(format_value(&json!([1, "x"])), "[1, 'x']");
    }

    #[test]
    fn format_in_list_and_empty_sentinel() {
        assert_eq!(format_in_list(&[json!(1), json!("a")]), "(1, 'a')");
        assert_eq!(format_in_list(&[]), "(NULL)");
    }

    #[test]
    fn valid_identifier_rules() {
        assert!(valid_identifier("col_1"));
        assert!(!valid_identifier("1col"));
        assert!(!valid_identifier("has space"));
        assert!(!valid_identifier(""));
    }
}
