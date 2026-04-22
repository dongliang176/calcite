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

import java.util.Objects;

/**
 * Immutable reference to a column in a table (or subquery alias).
 *
 * <p>The {@code database} field is optional; {@code table} and {@code column}
 * are always required.
 *
 * <p>Special constant values:
 * <ul>
 *   <li>{@link #STAR} – represents {@code SELECT *} wildcard columns.</li>
 *   <li>{@link #UNKNOWN_TABLE} – used when the originating table cannot be
 *       statically determined (e.g. generated columns).</li>
 * </ul>
 */
public final class ColumnRef {

  /** Wildcard column name used for {@code SELECT *}. */
  public static final String STAR = "*";

  /** Placeholder table name when the source table is unknown. */
  public static final String UNKNOWN_TABLE = "<unknown>";

  private final /* @Nullable */ String database;
  private final String table;
  private final String column;

  /**
   * Creates a column reference with an optional database qualifier.
   *
   * @param database schema / database name, or {@code null}
   * @param table    table name or subquery alias
   * @param column   column name
   */
  public ColumnRef(/* @Nullable */ String database, String table, String column) {
    this.database = database;
    this.table  = Objects.requireNonNull(table,  "table");
    this.column = Objects.requireNonNull(column, "column");
  }

  /**
   * Creates a column reference without a database qualifier.
   *
   * @param table  table name or subquery alias
   * @param column column name
   */
  public ColumnRef(String table, String column) {
    this(null, table, column);
  }

  /** Returns the database / schema name, or {@code null} if unspecified. */
  public /* @Nullable */ String getDatabase() {
    return database;
  }

  /** Returns the table name or subquery alias. */
  public String getTable() {
    return table;
  }

  /** Returns the column name. */
  public String getColumn() {
    return column;
  }

  /**
   * Returns the fully-qualified name as a dot-separated string.
   *
   * <p>Examples: {@code "mydb.orders.amount"}, {@code "orders.amount"}.
   */
  public String getFullName() {
    StringBuilder sb = new StringBuilder();
    if (database != null) {
      sb.append(database).append('.');
    }
    sb.append(table).append('.').append(column);
    return sb.toString();
  }

  @Override
  public String toString() {
    return getFullName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ColumnRef)) {
      return false;
    }
    ColumnRef that = (ColumnRef) o;
    return Objects.equals(database, that.database)
        && table.equals(that.table)
        && column.equals(that.column);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, table, column);
  }
}
