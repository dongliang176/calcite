# meta-lineage

A **field-level SQL lineage** library that supports both standard SQL and full
Spark SQL dialect.

## Two extraction paths

| Path | Parser | Requires schema? | Best for |
|------|--------|-----------------|---------|
| **Spark Catalyst** | `CatalystSqlParser` (no SparkSession) | No | Spark SQL dialect, Hive UDFs, `LATERAL VIEW`, CTEs |
| **Apache Calcite** | `SqlParser` + `SqlToRelConverter` | Yes (in-memory) | Standard ANSI SQL with precise type/origin metadata |

---

## Architecture

```
                    ┌─────────────────────────────────┐
                    │         SQL string               │
                    └────────────┬────────────────────┘
                                 │
          ┌──────────────────────┼─────────────────────────┐
          │ Spark Catalyst path  │                          │ Calcite path
          ▼                      │                          ▼
  CatalystSqlParser              │                  SqlParser.parseQuery
  (no SparkSession)              │                  SqlToRelConverter
          │                      │                  RelMetadataQuery
          ▼                      │                          │
  LogicalPlan tree               │                  RelNode tree
  (unresolved)                   │                  (column origins)
          │                      │                          │
          └──────────────────────┼─────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────────────┐
                    │        LineageResult             │
                    │   List<FieldLineage>             │
                    │     target  ← sources            │
                    │     TransformType                │
                    └─────────────────────────────────┘
```

### Data model

| Class | Description |
|-------|-------------|
| `ColumnRef` | Reference to a specific column: `(database?, table, column)` |
| `FieldLineage` | One output field: `target ← List<source>  [TransformType]` |
| `LineageResult` | All fields for one SQL statement + optional DML target table |

### `TransformType` values

| Value | Meaning |
|-------|---------|
| `DIRECT` | Column copied 1-to-1 from a source column |
| `DERIVED` | Column computed from ≥ 1 source columns via an expression |
| `AGGREGATED` | Column is the result of an aggregate function (`SUM`, `COUNT`, …) |
| `CONSTANT` | Column is a literal constant, no source columns |

---

## Quick start

### Maven coordinates

```xml
<dependency>
  <groupId>org.apache.calcite</groupId>
  <artifactId>meta-lineage</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Spark Catalyst extractor (no schema needed)

```java
String sql =
    "SELECT o.id, o.price * o.qty AS total, c.name "
    + "FROM orders o JOIN customers c ON o.customer_id = c.id";

LineageResult result = SparkSqlLineageExtractor.extract(sql, "result");

result.getFieldLineages().forEach(System.out::println);
// result.total  <- [orders.price, orders.qty]  [DERIVED]
// result.name   <- [customers.name]             [DIRECT]
```

### Calcite extractor (schema required)

```java
// 1. Define your schema
Map<String, List<String>> tables = new LinkedHashMap<>();
tables.put("orders",    Arrays.asList("id", "customer_id", "price", "qty"));
tables.put("customers", Arrays.asList("id", "name", "email"));
SchemaPlus schema = CalciteLineageExtractor.buildSchema(tables);

// 2. Extract
String sql =
    "SELECT o.id AS order_id, c.name, o.price * o.qty AS total "
    + "FROM orders o JOIN customers c ON o.customer_id = c.id";

LineageResult result = CalciteLineageExtractor.extract(sql, schema, "result");
result.getFieldLineages().forEach(System.out::println);
```

---

## Build

```bash
cd meta-lineage
mvn clean test
```

> **Java 11+** is required.  The Spark dependency is scoped as `provided` in
> `pom.xml`; add it to the `compile` scope when building a standalone fat-jar.

---

## Supported SQL constructs

| Construct | Spark Catalyst | Calcite |
|-----------|:--------------:|:-------:|
| `SELECT col`, `SELECT col AS alias` | ✅ | ✅ |
| Arithmetic / string expressions | ✅ | ✅ |
| `JOIN` (INNER / LEFT / RIGHT / FULL) | ✅ | ✅ |
| Subqueries | ✅ | ✅ |
| `WITH` (CTE) | ✅ | ✅ |
| `GROUP BY` + aggregate functions | ✅ | ✅ |
| `UNION` / `UNION ALL` | ✅ | ✅ |
| `INSERT INTO … SELECT` | ✅ | ✅ |
| `LATERAL VIEW` (Hive) | ✅ | ❌ |
| Spark-specific syntax | ✅ | ❌ |
