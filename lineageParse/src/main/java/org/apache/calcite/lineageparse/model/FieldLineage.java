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
package org.apache.calcite.lineageparse.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Describes the column-level lineage relationship for a single output field.
 *
 * <p>A {@code FieldLineage} records:
 * <ul>
 *   <li>{@link #target} – the output column being produced.</li>
 *   <li>{@link #sources} – the input columns that contribute to the output.</li>
 *   <li>{@link #transformType} – how the target is derived from the sources.</li>
 * </ul>
 *
 * <p>Example – {@code SELECT a + b AS total FROM t}:
 * <pre>
 *   target        = ColumnRef("result", "total")
 *   sources       = [ColumnRef("t","a"), ColumnRef("t","b")]
 *   transformType = DERIVED
 * </pre>
 *
 * <p>Example – {@code SELECT id FROM orders}:
 * <pre>
 *   target        = ColumnRef("result", "id")
 *   sources       = [ColumnRef("orders","id")]
 *   transformType = DIRECT
 * </pre>
 */
public final class FieldLineage {

  /**
   * Describes how the target column is computed from its sources.
   */
  public enum TransformType {
    /**
     * The target column is copied directly from exactly one source column
     * with no transformation, e.g. {@code SELECT a FROM t}.
     */
    DIRECT,

    /**
     * The target column is computed from one or more source columns via an
     * expression or scalar function, e.g. {@code a + b} or {@code UPPER(a)}.
     */
    DERIVED,

    /**
     * The target column is the result of an aggregate function such as
     * {@code COUNT}, {@code SUM}, {@code MAX}, etc.
     */
    AGGREGATED,

    /**
     * The target column is a constant literal with no source columns,
     * e.g. {@code SELECT 42 AS const_col}.
     */
    CONSTANT
  }

  private final ColumnRef target;
  private final List<ColumnRef> sources;
  private final TransformType transformType;

  /**
   * Creates a {@code FieldLineage}.
   *
   * @param target        the output (target) column
   * @param sources       the input (source) columns; may be empty for constants
   * @param transformType how the target is derived from the sources
   */
  public FieldLineage(ColumnRef target, List<ColumnRef> sources,
      TransformType transformType) {
    this.target        = Objects.requireNonNull(target, "target");
    this.sources       = Collections.unmodifiableList(
        Objects.requireNonNull(sources, "sources"));
    this.transformType = Objects.requireNonNull(transformType, "transformType");
  }

  /** Returns the target column reference. */
  public ColumnRef getTarget() {
    return target;
  }

  /** Returns an unmodifiable list of source column references. */
  public List<ColumnRef> getSources() {
    return sources;
  }

  /** Returns the type of transformation applied to produce the target column. */
  public TransformType getTransformType() {
    return transformType;
  }

  @Override
  public String toString() {
    return target.getFullName() + " <- " + sources + "  [" + transformType + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FieldLineage)) {
      return false;
    }
    FieldLineage that = (FieldLineage) o;
    return target.equals(that.target)
        && sources.equals(that.sources)
        && transformType == that.transformType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(target, sources, transformType);
  }
}
