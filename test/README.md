# Testing this extension

The `sql` directory contains the extension's [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). Add focused SQLLogicTests for SQL-visible behavior and use the parity tools documented in the root README for generated-data compatibility.

Run the release tests with:

```bash
make test
```

For a debug build, use:

```bash
make test_debug
```
