# PPL Breaking Changes

## Overview

This document tracks breaking changes and behavioral differences when upgrading from PPL 2.x (legacy parser) to PPL 3.5 (unified parser). The unified parser is based on Apache Calcite and tracks the latest OpenSearch PPL 3.5 specification.

---

## Version 3.5

### EXPLAIN Command Syntax Changed

The EXPLAIN command syntax has changed from Spark-style modes to OpenSearch PPL standard modes.

**Legacy Syntax (PPL 2.x)**:
```sql
EXPLAIN SIMPLE | source = accounts | where age > 18
EXPLAIN EXTENDED | source = accounts | where age > 18
EXPLAIN CODEGEN | source = accounts | where age > 18
EXPLAIN COST | source = accounts | where age > 18
EXPLAIN FORMATTED | source = accounts | where age > 18
```

**New Syntax (PPL 3.5)**:
```sql
EXPLAIN [<mode>] <queryStatement>
```

Supported modes: `standard` (default), `simple`, `cost`, `extended`

**Migration**: Update EXPLAIN commands to use the new syntax. The `FORMATTED` and `CODEGEN` modes are no longer supported.

**Reference**: [OpenSearch PPL EXPLAIN Command](https://docs.opensearch.org/latest/sql-and-ppl/ppl/commands/explain/)

---

### DESCRIBE Command Not Supported

The DESCRIBE command for querying table metadata is not available in the unified parser.

**Legacy Syntax (PPL 2.x)**:
```sql
DESCRIBE accounts
DESCRIBE default.accounts
```

**Workaround**: Use Spark SQL's DESCRIBE command directly:
```sql
DESCRIBE TABLE accounts
```

**Reference**: [OpenSearch PPL DESCRIBE Command](https://docs.opensearch.org/latest/sql-and-ppl/ppl/commands/describe/)

---

### Multiple Table Search Not Supported

Searching multiple tables in a single source command is not supported.

**Legacy Syntax (PPL 2.x)**:
```sql
source = table1, table2 | where country = "USA"
source = table1, table2 as t | where t.country = "USA"
```

**Workaround**: Use explicit UNION queries or wait for unified parser support.

---

## Reporting Issues

If you encounter unexpected behavior after upgrading to unified PPL, please report it at:
[opensearch-spark GitHub Issues](https://github.com/opensearch-project/opensearch-spark/issues)
