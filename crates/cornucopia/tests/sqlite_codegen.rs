use cornucopia::{generate_live_with_sqlite, CodegenSettings};
use std::io::Write;
use std::process::{Command, Stdio};

fn format(source: &str) -> String {
    let source = prettyplease::unparse(&syn::parse_file(source).unwrap());
    let mut formatter = Command::new("rustfmt")
        .args(["--edition", "2021", "--emit", "stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    formatter
        .stdin
        .take()
        .unwrap()
        .write_all(source.as_bytes())
        .unwrap();
    let output = formatter.wait_with_output().unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap()
}

#[test]
#[ignore = "requires DATABASE_URL pointing to a PostgreSQL test database"]
fn transaction_query_fixture_matches_generator() {
    let mut postgres = postgres::Client::connect(
        &std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
        postgres::NoTls,
    )
    .unwrap();
    postgres
        .batch_execute(
            "CREATE TEMP TABLE items (value BIGINT NOT NULL UNIQUE, label TEXT NOT NULL)",
        )
        .unwrap();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite
        .execute_batch("CREATE TABLE items (value BIGINT NOT NULL UNIQUE, label TEXT NOT NULL)")
        .unwrap();
    let queries =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../client_async/tests/fixtures");
    let generated = generate_live_with_sqlite(
        &mut postgres,
        queries,
        None,
        &sqlite,
        CodegenSettings {
            gen_async: true,
            gen_sync: false,
            gen_sqlite: true,
            derive_ser: false,
        },
    )
    .unwrap();
    let expected = include_str!("../../client_async/tests/fixtures/queries.rs");
    assert_eq!(format(&generated), format(expected));
}
