/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.lineageparse;

import org.apache.calcite.lineageparse.model.ColumnRef;
import org.apache.calcite.lineageparse.model.FieldLineage;
import org.apache.calcite.lineageparse.model.FieldLineage.TransformType;
import org.apache.calcite.lineageparse.model.LineageResult;
import org.apache.calcite.lineageparse.spark.SparkSqlLineageExtractor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SparkSqlLineageExtractor}.
 *
 * <p>These tests use the Spark Catalyst parser only – no SparkSession or
 * execution engine is required.
 */
class SparkSqlLineageExtractorTest {

  // ─────────────────────────────────────────────────────────────────────────
  // 1. Simple SELECT with column alias
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("Simple SELECT: direct column and aliased expression")
  void testSimpleSelect() {
    String sql = "SELECT id, name AS customer_name FROM customers";
    LineageResult result = SparkSqlLineageExtractor.extract(sql, "result");

    System.out.println("=== testSimpleSelect ===");
    System.out.println(result);

    assertNotNull(result);
    assertFalse(result.getFieldLineages().isEmpty(),
        "Should have at least one field lineage entry");

    // 'id' should come from 'customers'
    Optional<FieldLineage> idLineage = result.getLineageFor("id");
    assertTrue(idLineage.isPresent(), "Expected lineage for 'id'");
    assertSourceTable(idLineage.get(), "customers");

    // 'customer_name' should be an alias for 'name' from 'customers'
    Optional<FieldLineage> nameLineage = result.getLineageFor("customer_name");
    assertTrue(nameLineage.isPresent(), "Expected lineage for 'customer_name'");
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 2. Expression (DERIVED)
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("Expression column: price * qty AS total → DERIVED")
  void testDerivedExpression() {
    String sql = "SELECT id, price * qty AS total FROM orders";
    LineageResult result = SparkSqlLineageExtractor.extract(sql, "result");

    System.out.println("=== testDerivedExpression ===");
    System.out.println(result);

    Optional<FieldLineage> totalLineage = result.getLineageFor("total");
    assertTrue(totalLineage.isPresent(), "Expected lineage for 'total'");

    FieldLineage fl = totalLineage.get();
    assertEquals(TransformType.DERIVED, fl.getTransformType(),
        "'total' should be DERIVED");
    // Both 'price' and 'qty' should be sources
    List<ColumnRef> sources = fl.getSources();
    assertTrue(sources.size() >= 2,
        "Expected at least 2 sources for 'price * qty', got: " + sources);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 3. JOIN
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("JOIN: columns from both sides of the join")
  void testJoin() {
    String sql =
        "SELECT o.id AS order_id, c.name AS customer_name "
        + "FROM orders o JOIN customers c ON o.customer_id = c.id";
    LineageResult result = SparkSqlLineageExtractor.extract(sql, "result");

    System.out.println("=== testJoin ===");
    System.out.println(result);

    assertNotNull(result);
    assertFalse(result.getFieldLineages().isEmpty());

    // order_id → orders (Catalyst resolves alias 'o' → underlying table 'orders')
    Optional<FieldLineage> orderIdLineage = result.getLineageFor("order_id");
    assertTrue(orderIdLineage.isPresent(), "Expected lineage for 'order_id'");
    // Sources should reference either the alias 'o' or the actual table 'orders'
    List<ColumnRef> orderIdSources = orderIdLineage.get().getSources();
    boolean fromOrders = orderIdSources.stream()
        .anyMatch(r -> r.getTable().equalsIgnoreCase("o")
            || r.getTable().equalsIgnoreCase("orders"));
    assertTrue(fromOrders,
        "order_id should come from orders (or alias o), got: " + orderIdSources);

    // customer_name → customers (alias 'c')
    Optional<FieldLineage> custNameLineage = result.getLineageFor("customer_name");
    assertTrue(custNameLineage.isPresent(), "Expected lineage for 'customer_name'");
    List<ColumnRef> custSources = custNameLineage.get().getSources();
    boolean fromCustomers = custSources.stream()
        .anyMatch(r -> r.getTable().equalsIgnoreCase("c")
            || r.getTable().equalsIgnoreCase("customers"));
    assertTrue(fromCustomers,
        "customer_name should come from customers (or alias c), got: " + custSources);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 4. Subquery
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("Subquery: column lineage traces through inner SELECT")
  void testSubquery() {
    String sql =
        "SELECT sub.order_id, sub.total "
        + "FROM (SELECT id AS order_id, price * qty AS total FROM orders) sub";
    LineageResult result = SparkSqlLineageExtractor.extract(sql, "result");

    System.out.println("=== testSubquery ===");
    System.out.println(result);

    assertNotNull(result);
    assertFalse(result.getFieldLineages().isEmpty());
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 5. Aggregate
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("Aggregate: COUNT(*) AS cnt → AGGREGATED")
  void testAggregate() {
    String sql =
        "SELECT department, COUNT(*) AS cnt, SUM(salary) AS total_salary "
        + "FROM employees GROUP BY department";
    LineageResult result = SparkSqlLineageExtractor.extract(sql, "result");

    System.out.println("=== testAggregate ===");
    System.out.println(result);

    assertNotNull(result);

    Optional<FieldLineage> cntLineage = result.getLineageFor("cnt");
    assertTrue(cntLineage.isPresent(), "Expected lineage for 'cnt'");
    assertEquals(TransformType.AGGREGATED, cntLineage.get().getTransformType(),
        "'cnt' should be AGGREGATED");

    Optional<FieldLineage> salaryLineage = result.getLineageFor("total_salary");
    assertTrue(salaryLineage.isPresent(), "Expected lineage for 'total_salary'");
    assertEquals(TransformType.AGGREGATED, salaryLineage.get().getTransformType(),
        "'total_salary' should be AGGREGATED");
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 6. UNION ALL
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("UNION ALL: sources from both branches")
  void testUnionAll() {
    String sql =
        "SELECT id, name FROM customers_us "
        + "UNION ALL "
        + "SELECT id, name FROM customers_eu";
    LineageResult result = SparkSqlLineageExtractor.extract(sql, "result");

    System.out.println("=== testUnionAll ===");
    System.out.println(result);

    assertNotNull(result);
    assertFalse(result.getFieldLineages().isEmpty());

    Optional<FieldLineage> nameLineage = result.getLineageFor("name");
    assertTrue(nameLineage.isPresent(), "Expected lineage for 'name'");
    // Should reference both tables
    List<ColumnRef> sources = nameLineage.get().getSources();
    assertTrue(sources.size() >= 2,
        "UNION ALL 'name' should have sources from both branches, got: " + sources);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 7. CTE (WITH clause)
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("CTE: WITH clause resolves inline")
  void testCte() {
    String sql =
        "WITH high_value AS ("
        + "  SELECT id, amount FROM orders WHERE amount > 1000"
        + ") "
        + "SELECT id, amount FROM high_value";
    LineageResult result = SparkSqlLineageExtractor.extract(sql, "result");

    System.out.println("=== testCte ===");
    System.out.println(result);

    assertNotNull(result);
    assertFalse(result.getFieldLineages().isEmpty());
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 8. Constant literal (CONSTANT transform type)
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("Constant literal: SELECT 1 AS const_col → CONSTANT")
  void testConstant() {
    String sql = "SELECT id, 'FIXED' AS status FROM orders";
    LineageResult result = SparkSqlLineageExtractor.extract(sql, "result");

    System.out.println("=== testConstant ===");
    System.out.println(result);

    assertNotNull(result);

    Optional<FieldLineage> statusLineage = result.getLineageFor("status");
    assertTrue(statusLineage.isPresent(), "Expected lineage for 'status'");
    assertEquals(TransformType.CONSTANT, statusLineage.get().getTransformType(),
        "'status' should be CONSTANT");
    assertTrue(statusLineage.get().getSources().isEmpty(),
        "CONSTANT column should have no sources");
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 9. toString / pretty-print
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("LineageResult.toString() should produce non-empty output")
  void testToString() {
    String sql = "SELECT id, name FROM customers";
    LineageResult result = SparkSqlLineageExtractor.extract(sql, "result");
    String str = result.toString();
    assertFalse(str.isBlank(), "toString() should not be blank");
    assertTrue(str.contains("LineageResult"), "toString() should contain 'LineageResult'");
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Helpers
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Asserts that at least one source in the field lineage refers to the
   * expected table (case-insensitive).
   */
  private static void assertSourceTable(FieldLineage fl, String expectedTable) {
    List<ColumnRef> sources = fl.getSources();
    boolean found = sources.stream()
        .anyMatch(r -> r.getTable().equalsIgnoreCase(expectedTable));
    assertTrue(found,
        "Expected at least one source from table '" + expectedTable
        + "' in " + sources + " for target '" + fl.getTarget() + "'");
  }
}
