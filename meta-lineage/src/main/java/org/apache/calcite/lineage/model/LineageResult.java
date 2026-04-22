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
package org.apache.calcite.lineage.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The complete field-level lineage result for a single SQL statement.
 *
 * <p>Contains:
 * <ul>
 *   <li>The original SQL text.</li>
 *   <li>One {@link FieldLineage} entry per output column.</li>
 *   <li>An optional DML target table (for {@code INSERT INTO …} or
 *       {@code CREATE TABLE … AS SELECT}).</li>
 * </ul>
 *
 * <p>Usage example:
 * <pre>{@code
 * LineageResult result = SparkSqlLineageExtractor.extract(
 *         "INSERT INTO orders SELECT id, price * qty AS total FROM items",
 *         "orders");
 *
 * result.getFieldLineages().forEach(System.out::println);
 * }</pre>
 */
public final class LineageResult {

  private final String sql;
  private final /* @Nullable */ ColumnRef targetTable;
  private final List<FieldLineage> fieldLineages;

  /**
   * Creates a {@code LineageResult}.
   *
   * @param sql           original SQL text
   * @param fieldLineages one entry per output column
   * @param targetTable   DML target table, or {@code null} for plain SELECT
   */
  public LineageResult(String sql, List<FieldLineage> fieldLineages,
      /* @Nullable */ ColumnRef targetTable) {
    this.sql           = Objects.requireNonNull(sql, "sql");
    this.fieldLineages = Collections.unmodifiableList(
        Objects.requireNonNull(fieldLineages, "fieldLineages"));
    this.targetTable   = targetTable;
  }

  /** Returns the original SQL string. */
  public String getSql() {
    return sql;
  }

  /**
   * Returns the DML target table, or {@link Optional#empty()} for a plain
   * {@code SELECT} statement.
   */
  public Optional<ColumnRef> getTargetTable() {
    return Optional.ofNullable(targetTable);
  }

  /** Returns the unmodifiable list of field-level lineage entries. */
  public List<FieldLineage> getFieldLineages() {
    return fieldLineages;
  }

  /**
   * Returns the {@link FieldLineage} whose target column matches
   * {@code columnName} (case-insensitive), or {@link Optional#empty()}.
   */
  public Optional<FieldLineage> getLineageFor(String columnName) {
    return fieldLineages.stream()
        .filter(fl -> fl.getTarget().getColumn().equalsIgnoreCase(columnName))
        .findFirst();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("LineageResult for: ").append(abbreviate(sql)).append('\n');
    if (targetTable != null) {
      sb.append("  Target table : ").append(targetTable.getTable()).append('\n');
    }
    for (FieldLineage fl : fieldLineages) {
      sb.append("  ").append(fl).append('\n');
    }
    return sb.toString();
  }

  private static String abbreviate(String s) {
    String single = s.replaceAll("\\s+", " ").trim();
    return single.length() > 80 ? single.substring(0, 77) + "..." : single;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LineageResult)) {
      return false;
    }
    LineageResult that = (LineageResult) o;
    return sql.equals(that.sql)
        && Objects.equals(targetTable, that.targetTable)
        && fieldLineages.equals(that.fieldLineages);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql, targetTable, fieldLineages);
  }
}
