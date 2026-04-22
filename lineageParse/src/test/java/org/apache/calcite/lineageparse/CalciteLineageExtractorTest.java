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

import org.apache.calcite.lineageparse.calcite.CalciteLineageExtractor;
import org.apache.calcite.lineageparse.model.ColumnRef;
import org.apache.calcite.lineageparse.model.FieldLineage;
import org.apache.calcite.lineageparse.model.FieldLineage.TransformType;
import org.apache.calcite.lineageparse.model.LineageResult;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CalciteLineageExtractor}.
 *
 * <p>All tests register a small in-memory schema and verify that the
 * Calcite column-origin metadata correctly traces output fields back to
 * their source table columns.
 */
class CalciteLineageExtractorTest {

  private SchemaPlus schema;

  @BeforeEach
  void buildSchema() {
    Map<String, List<String>> tables = new LinkedHashMap<>();
    tables.put("orders",    Arrays.asList("id", "customer_id", "price", "qty", "amount"));
    tables.put("customers", Arrays.asList("id", "name", "email", "department", "salary"));
    tables.put("items",     Arrays.asList("id", "name", "price"));
    schema = CalciteLineageExtractor.buildSchema(tables);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 1. Simple SELECT
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("Simple SELECT: DIRECT lineage for plain column references")
  void testSimpleSelect() throws Exception {
    String sql = "SELECT id, name FROM customers";
    LineageResult result = CalciteLineageExtractor.extract(sql, schema, "result");

    System.out.println("=== Calcite testSimpleSelect ===");
    System.out.println(result);

    assertNotNull(result);
    assertEquals(2, result.getFieldLineages().size());

    FieldLineage idFl = result.getFieldLineages().get(0);
    assertTrue(idFl.getTarget().getColumn().equalsIgnoreCase("id"),
        "First field should be 'id', got: " + idFl.getTarget().getColumn());
    assertEquals(TransformType.DIRECT, idFl.getTransformType());
    assertSourceTable(idFl, "customers");

    FieldLineage nameFl = result.getFieldLineages().get(1);
    assertTrue(nameFl.getTarget().getColumn().equalsIgnoreCase("name"),
        "Second field should be 'name', got: " + nameFl.getTarget().getColumn());
    assertEquals(TransformType.DIRECT, nameFl.getTransformType());
    assertSourceTable(nameFl, "customers");
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 2. Derived expression
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("Expression column: price * qty AS total → DERIVED, sources from orders")
  void testDerivedExpression() throws Exception {
    String sql = "SELECT id, price * qty AS total FROM orders";
    LineageResult result = CalciteLineageExtractor.extract(sql, schema, "result");

    System.out.println("=== Calcite testDerivedExpression ===");
    System.out.println(result);

    Optional<FieldLineage> totalFl = result.getLineageFor("total");
    assertTrue(totalFl.isPresent(), "Expected lineage for 'total'");

    FieldLineage fl = totalFl.get();
    assertEquals(TransformType.DERIVED, fl.getTransformType(),
        "price * qty should be DERIVED");
    List<ColumnRef> sources = fl.getSources();
    assertTrue(sources.size() >= 2,
        "Expected >= 2 sources for 'price * qty', got: " + sources);
    assertSourceTable(fl, "orders");
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 3. JOIN
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("JOIN: each output column maps to correct source table")
  void testJoin() throws Exception {
    String sql =
        "SELECT o.id AS order_id, c.name AS customer_name "
        + "FROM orders o JOIN customers c ON o.customer_id = c.id";
    LineageResult result = CalciteLineageExtractor.extract(sql, schema, "result");

    System.out.println("=== Calcite testJoin ===");
    System.out.println(result);

    assertNotNull(result);
    assertEquals(2, result.getFieldLineages().size());

    Optional<FieldLineage> orderIdFl = result.getFieldLineages().stream()
        .filter(fl -> fl.getTarget().getColumn().equalsIgnoreCase("order_id"))
        .findFirst();
    assertTrue(orderIdFl.isPresent(), "Expected lineage for 'order_id'");
    assertSourceTable(orderIdFl.get(), "orders");

    Optional<FieldLineage> custNameFl = result.getFieldLineages().stream()
        .filter(fl -> fl.getTarget().getColumn().equalsIgnoreCase("customer_name"))
        .findFirst();
    assertTrue(custNameFl.isPresent(), "Expected lineage for 'customer_name'");
    assertSourceTable(custNameFl.get(), "customers");
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 4. Subquery
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("Subquery: lineage passes through inner SELECT")
  void testSubquery() throws Exception {
    String sql =
        "SELECT sub.order_id, sub.total "
        + "FROM (SELECT id AS order_id, price * qty AS total FROM orders) sub";
    LineageResult result = CalciteLineageExtractor.extract(sql, schema, "result");

    System.out.println("=== Calcite testSubquery ===");
    System.out.println(result);

    assertNotNull(result);
    assertFalse(result.getFieldLineages().isEmpty());

    Optional<FieldLineage> orderIdFl = result.getFieldLineages().stream()
        .filter(fl -> fl.getTarget().getColumn().equalsIgnoreCase("order_id"))
        .findFirst();
    assertTrue(orderIdFl.isPresent(), "Expected lineage for 'order_id'");
    assertSourceTable(orderIdFl.get(), "orders");
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 5. Aggregate
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("Aggregate: SUM(price) AS total_price → DERIVED (Calcite marks as derived)")
  void testAggregate() throws Exception {
    String sql =
        "SELECT customer_id, SUM(price) AS total_price "
        + "FROM orders GROUP BY customer_id";
    LineageResult result = CalciteLineageExtractor.extract(sql, schema, "result");

    System.out.println("=== Calcite testAggregate ===");
    System.out.println(result);

    assertNotNull(result);
    assertFalse(result.getFieldLineages().isEmpty());

    // The grouping key should be DIRECT
    Optional<FieldLineage> groupKeyFl = result.getFieldLineages().stream()
        .filter(fl -> fl.getTarget().getColumn().equalsIgnoreCase("customer_id"))
        .findFirst();
    assertTrue(groupKeyFl.isPresent(), "Expected lineage for 'customer_id'");
    assertEquals(TransformType.DIRECT, groupKeyFl.get().getTransformType());
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 6. UNION ALL
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @DisplayName("UNION ALL: sources from both branches")
  void testUnionAll() throws Exception {
    String sql =
        "SELECT id, name FROM customers "
        + "UNION ALL "
        + "SELECT id, name FROM items";
    LineageResult result = CalciteLineageExtractor.extract(sql, schema, "result");

    System.out.println("=== Calcite testUnionAll ===");
    System.out.println(result);

    assertNotNull(result);
    assertFalse(result.getFieldLineages().isEmpty());
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Helpers
  // ─────────────────────────────────────────────────────────────────────────

  private static void assertSourceTable(FieldLineage fl, String expectedTable) {
    List<ColumnRef> sources = fl.getSources();
    boolean found = sources.stream()
        .anyMatch(r -> r.getTable().equalsIgnoreCase(expectedTable));
    assertTrue(found,
        "Expected at least one source from table '" + expectedTable
        + "' in " + sources + " for target '" + fl.getTarget() + "'");
  }
}
