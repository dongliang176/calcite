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
package org.apache.calcite.lineage.spark;

import org.apache.calcite.lineage.model.ColumnRef;
import org.apache.calcite.lineage.model.FieldLineage;
import org.apache.calcite.lineage.model.FieldLineage.TransformType;
import org.apache.calcite.lineage.model.LineageResult;

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.jdk.CollectionConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts <strong>field-level lineage</strong> from Spark SQL statements using
 * the <em>Spark Catalyst</em> parser and logical plan tree.
 *
 * <h2>Supported constructs</h2>
 * <ul>
 *   <li>Simple {@code SELECT} with column references and expressions</li>
 *   <li>{@code SELECT … AS alias}</li>
 *   <li>{@code JOIN} (INNER / LEFT / RIGHT / FULL)</li>
 *   <li>Subqueries (nested {@code SELECT})</li>
 *   <li>{@code AGGREGATE} ({@code GROUP BY}, {@code COUNT}, {@code SUM}, …)</li>
 *   <li>{@code UNION} / {@code UNION ALL}</li>
 *   <li>{@code INSERT INTO target SELECT …} (supply target table name)</li>
 *   <li>{@code WITH} (CTE) – resolved inline by Catalyst before analysis</li>
 * </ul>
 *
 * <h2>Design</h2>
 * <p>This extractor <em>does not start a full SparkSession</em>; it only uses
 * the Catalyst <strong>parser</strong> ({@code CatalystSqlParser}) and then
 * walks the resulting unresolved {@link LogicalPlan} tree.  Because the plan is
 * unresolved (no schema catalog), column references remain as
 * {@link UnresolvedAttribute} nodes.  We propagate those references upward
 * through the plan using a scope map from <em>output column name</em> to
 * <em>list of {@link ColumnRef}s</em>.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * String sql = "SELECT o.id, o.price * o.qty AS total "
 *            + "FROM orders o JOIN items i ON o.item_id = i.id";
 * LineageResult result = SparkSqlLineageExtractor.extract(sql, "result");
 * result.getFieldLineages().forEach(System.out::println);
 * }</pre>
 */
public final class SparkSqlLineageExtractor {

  private static final Logger LOG =
      LoggerFactory.getLogger(SparkSqlLineageExtractor.class);

  /** Output table name used when no DML target is specified. */
  private static final String DEFAULT_OUTPUT = "result";

  private SparkSqlLineageExtractor() {
    // utility class
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Public API
  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Parses {@code sql} with the Spark Catalyst parser and returns the
   * field-level lineage for every output column.
   *
   * @param sql         Spark SQL statement (SELECT, INSERT INTO … SELECT, CTE, …)
   * @param outputTable name to use for the output / target table in the result
   * @return            field-level lineage result
   */
  public static LineageResult extract(String sql, String outputTable) {
    LogicalPlan plan = CatalystSqlParser$.MODULE$.parsePlan(sql);
    LOG.debug("Parsed plan:\n{}", plan);

    // Detect DML target (InsertIntoStatement wraps the child SELECT)
    String targetTableName = null;
    LogicalPlan queryPlan = plan;

    // Spark 3.x represents INSERT INTO as InsertIntoStatement or
    // InsertIntoHadoopFsRelationCommand.  We unwrap it by name because the
    // exact class lives in spark-sql (not catalyst).
    String planClass = plan.getClass().getSimpleName();
    if (planClass.startsWith("InsertInto") || planClass.startsWith("CreateTableAs")) {
      // Try to get the target table name via reflection to avoid hard
      // dependency on spark-sql classes.
      targetTableName = extractDmlTarget(plan);
      // child() or query() holds the actual SELECT plan
      queryPlan = firstChild(plan);
    }

    // Walk the query plan
    Map<String, List<ColumnRef>> scope = new LinkedHashMap<>();
    Map<String, TransformType> typeMap = new LinkedHashMap<>();
    walkPlan(queryPlan, scope, typeMap);

    // Build FieldLineage list
    String effectiveOutput = targetTableName != null ? targetTableName : outputTable;
    List<FieldLineage> lineages = buildLineages(scope, typeMap, effectiveOutput);

    ColumnRef targetRef = targetTableName != null
        ? new ColumnRef(targetTableName, ColumnRef.STAR) : null;

    return new LineageResult(sql, lineages, targetRef);
  }

  /**
   * Convenience overload – uses {@code "result"} as the output table name.
   */
  public static LineageResult extract(String sql) {
    return extract(sql, DEFAULT_OUTPUT);
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Internal plan walking
  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Recursively walks a (potentially unresolved) {@link LogicalPlan} and
   * populates {@code scope} with output-column → source-columns mappings,
   * and {@code typeMap} with output-column → transform-type mappings.
   */
  private static void walkPlan(LogicalPlan plan,
      Map<String, List<ColumnRef>> scope,
      Map<String, TransformType> typeMap) {

    String nodeType = plan.getClass().getSimpleName();
    LOG.trace("walkPlan node={}", nodeType);

    switch (nodeType) {

      case "Project":
        handleProject((Project) plan, scope, typeMap);
        break;

      case "Aggregate":
        handleAggregate((Aggregate) plan, scope, typeMap);
        break;

      case "Filter":
        // Filter does not change the output schema – propagate child scope
        walkPlan(((Filter) plan).child(), scope, typeMap);
        break;

      case "Join":
        handleJoin((Join) plan, scope, typeMap);
        break;

      case "SubqueryAlias":
        handleSubqueryAlias((SubqueryAlias) plan, scope, typeMap);
        break;

      case "Union":
      case "Intersect":
      case "Except":
        handleSetOp(plan, scope, typeMap);
        break;

      // UnresolvedRelation → leaf table scan: columns unknown without catalog
      case "UnresolvedRelation":
        handleUnresolvedRelation(plan, scope);
        break;

      // Resolved leaf (e.g. LocalRelation in tests with InMemoryRelation)
      default:
        handleGenericLeaf(plan, scope, typeMap);
        break;
    }
  }

  // ── Project ──────────────────────────────────────────────────────────────

  private static void handleProject(Project project,
      Map<String, List<ColumnRef>> scope,
      Map<String, TransformType> typeMap) {
    // First walk the child to populate scope with the input columns
    Map<String, List<ColumnRef>> childScope = new LinkedHashMap<>();
    Map<String, TransformType> childTypeMap = new LinkedHashMap<>();
    walkPlan(project.child(), childScope, childTypeMap);

    // For every named expression in the project list, resolve its sources
    List<NamedExpression> projList =
        CollectionConverters.SeqHasAsJava(project.projectList()).asJava();
    for (NamedExpression ne : projList) {
      String outName = ne.name();
      List<ColumnRef> sources = resolveExpression((Expression) ne, childScope);
      TransformType tt = classifyExpression((Expression) ne, sources);
      List<ColumnRef> deduped = deduplicate(sources);
      scope.put(outName, deduped);
      typeMap.put(outName, tt);
    }
  }

  // ── Aggregate ────────────────────────────────────────────────────────────

  private static void handleAggregate(Aggregate agg,
      Map<String, List<ColumnRef>> scope,
      Map<String, TransformType> typeMap) {
    Map<String, List<ColumnRef>> childScope = new LinkedHashMap<>();
    Map<String, TransformType> childTypeMap = new LinkedHashMap<>();
    walkPlan(agg.child(), childScope, childTypeMap);

    // Build a set of grouping key names so we can distinguish group-by columns
    // (DIRECT/DERIVED) from aggregate calls (AGGREGATED).
    // In the unresolved plan, grouping expressions appear as UnresolvedAttribute
    // or Alias nodes; extracting their output name is sufficient.
    java.util.Set<String> groupingNames = new java.util.HashSet<>();
    List<Expression> groupingExprs =
        CollectionConverters.SeqHasAsJava(agg.groupingExpressions()).asJava();
    for (Expression ge : groupingExprs) {
      if (ge instanceof NamedExpression) {
        groupingNames.add(((NamedExpression) ge).name());
      } else if (ge instanceof UnresolvedAttribute) {
        List<String> parts =
            CollectionConverters.SeqHasAsJava(((UnresolvedAttribute) ge).nameParts()).asJava();
        if (!parts.isEmpty()) {
          groupingNames.add(parts.get(parts.size() - 1));
        }
      }
    }

    List<NamedExpression> aggExprs =
        CollectionConverters.SeqHasAsJava(agg.aggregateExpressions()).asJava();
    for (NamedExpression ne : aggExprs) {
      String outName = ne.name();
      List<ColumnRef> sources = resolveExpression((Expression) ne, childScope);
      // If this output name corresponds to a grouping key, it is DIRECT/DERIVED;
      // otherwise it must be an aggregate call → AGGREGATED.
      boolean isGroupingKey = groupingNames.contains(outName);
      TransformType tt = isGroupingKey
          ? classifyExpression((Expression) ne, sources)
          : TransformType.AGGREGATED;
      scope.put(outName, deduplicate(sources));
      typeMap.put(outName, tt);
    }
  }

  // ── Join ─────────────────────────────────────────────────────────────────

  private static void handleJoin(Join join,
      Map<String, List<ColumnRef>> scope,
      Map<String, TransformType> typeMap) {
    // Merge left and right child scopes
    Map<String, List<ColumnRef>> leftScope = new LinkedHashMap<>();
    Map<String, TransformType> leftTypeMap = new LinkedHashMap<>();
    Map<String, List<ColumnRef>> rightScope = new LinkedHashMap<>();
    Map<String, TransformType> rightTypeMap = new LinkedHashMap<>();
    walkPlan(join.left(), leftScope, leftTypeMap);
    walkPlan(join.right(), rightScope, rightTypeMap);
    scope.putAll(leftScope);
    scope.putAll(rightScope);
    typeMap.putAll(leftTypeMap);
    typeMap.putAll(rightTypeMap);
  }

  // ── SubqueryAlias ────────────────────────────────────────────────────────

  private static void handleSubqueryAlias(SubqueryAlias alias,
      Map<String, List<ColumnRef>> scope,
      Map<String, TransformType> typeMap) {
    String aliasName = alias.alias();
    Map<String, List<ColumnRef>> childScope = new LinkedHashMap<>();
    Map<String, TransformType> childTypeMap = new LinkedHashMap<>();
    walkPlan(alias.child(), childScope, childTypeMap);

    // Re-expose child columns under the alias name
    for (Map.Entry<String, List<ColumnRef>> e : childScope.entrySet()) {
      String qualKey = aliasName + "." + e.getKey();
      scope.put(qualKey, e.getValue());
      scope.putIfAbsent(e.getKey(), e.getValue());
      TransformType tt = childTypeMap.getOrDefault(e.getKey(), TransformType.DIRECT);
      typeMap.putIfAbsent(qualKey, tt);
      typeMap.putIfAbsent(e.getKey(), tt);
    }
    // Record this alias as a known table for resolution
    scope.put("__table__:" + aliasName,
        Collections.singletonList(new ColumnRef(aliasName, ColumnRef.STAR)));
  }

  // ── Set operations (UNION / INTERSECT / EXCEPT) ──────────────────────────

  private static void handleSetOp(LogicalPlan plan,
      Map<String, List<ColumnRef>> scope,
      Map<String, TransformType> typeMap) {
    List<LogicalPlan> children =
        CollectionConverters.SeqHasAsJava(plan.children()).asJava();
    if (children.isEmpty()) {
      return;
    }
    Map<String, List<ColumnRef>> firstScope = new LinkedHashMap<>();
    Map<String, TransformType> firstTypeMap = new LinkedHashMap<>();
    walkPlan(children.get(0), firstScope, firstTypeMap);

    for (int i = 1; i < children.size(); i++) {
      Map<String, List<ColumnRef>> sibling = new LinkedHashMap<>();
      Map<String, TransformType> siblingTypeMap = new LinkedHashMap<>();
      walkPlan(children.get(i), sibling, siblingTypeMap);
      // Merge sources for matching column names
      for (Map.Entry<String, List<ColumnRef>> e : sibling.entrySet()) {
        firstScope.merge(e.getKey(), e.getValue(), (a, b) -> {
          List<ColumnRef> merged = new ArrayList<>(a);
          for (ColumnRef r : b) {
            if (!merged.contains(r)) {
              merged.add(r);
            }
          }
          return merged;
        });
        firstTypeMap.putIfAbsent(e.getKey(), siblingTypeMap.get(e.getKey()));
      }
    }
    scope.putAll(firstScope);
    typeMap.putAll(firstTypeMap);
  }

  // ── Leaf: UnresolvedRelation (table scan) ─────────────────────────────────

  private static void handleUnresolvedRelation(LogicalPlan plan,
      Map<String, List<ColumnRef>> scope) {
    String tableName = ColumnRef.UNKNOWN_TABLE;
    try {
      scala.collection.Seq<?> parts = (scala.collection.Seq<?>)
          plan.getClass().getMethod("multipartIdentifier").invoke(plan);
      List<?> partList = CollectionConverters.SeqHasAsJava(parts).asJava();
      if (!partList.isEmpty()) {
        tableName = String.join(".", partList.stream()
            .map(Object::toString)
            .toArray(String[]::new));
      }
    } catch (Exception e) {
      LOG.warn("Cannot extract table name from {}: {}", plan.getClass(), e.getMessage());
    }
    scope.put(tableName + "." + ColumnRef.STAR,
        Collections.singletonList(new ColumnRef(tableName, ColumnRef.STAR)));
    scope.put("__table__:" + tableName,
        Collections.singletonList(new ColumnRef(tableName, ColumnRef.STAR)));
  }

  // ── Generic leaf ────────────────────────────────────────────────────────

  private static void handleGenericLeaf(LogicalPlan plan,
      Map<String, List<ColumnRef>> scope,
      Map<String, TransformType> typeMap) {
    try {
      List<?> output = CollectionConverters.SeqHasAsJava(plan.output()).asJava();
      String pseudoTable = plan.getClass().getSimpleName().toLowerCase();
      for (Object attr : output) {
        if (attr instanceof AttributeReference) {
          AttributeReference ar = (AttributeReference) attr;
          scope.putIfAbsent(ar.name(),
              Collections.singletonList(new ColumnRef(pseudoTable, ar.name())));
          typeMap.putIfAbsent(ar.name(), TransformType.DIRECT);
        }
      }
    } catch (Exception e) {
      LOG.debug("handleGenericLeaf: {}", e.getMessage());
    }

    List<LogicalPlan> children =
        CollectionConverters.SeqHasAsJava(plan.children()).asJava();
    for (LogicalPlan child : children) {
      walkPlan(child, scope, typeMap);
    }
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Expression resolution helpers
  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Collects all leaf column references inside {@code expr}, looking them up
   * in {@code childScope} to resolve to physical table columns.
   */
  private static List<ColumnRef> resolveExpression(Expression expr,
      Map<String, List<ColumnRef>> childScope) {
    List<ColumnRef> result = new ArrayList<>();
    collectColumnRefs(expr, childScope, result);
    return result;
  }

  /** Recursively collects all {@link ColumnRef}s referenced by {@code expr}. */
  private static void collectColumnRefs(Expression expr,
      Map<String, List<ColumnRef>> scope,
      List<ColumnRef> out) {

    if (expr instanceof UnresolvedAttribute) {
      // e.g.  a,  t.a,  db.t.a
      UnresolvedAttribute ua = (UnresolvedAttribute) expr;
      List<String> nameParts = CollectionConverters.SeqHasAsJava(ua.nameParts()).asJava();
      String colKey = String.join(".", nameParts);
      List<ColumnRef> resolved = resolveColumnKey(colKey, nameParts, scope);
      out.addAll(resolved);
      return;
    }

    if (expr instanceof AttributeReference) {
      AttributeReference ar = (AttributeReference) expr;
      List<ColumnRef> resolved = resolveColumnKey(ar.name(),
          Collections.singletonList(ar.name()), scope);
      out.addAll(resolved);
      return;
    }

    if (expr instanceof Alias) {
      // Unwrap the alias child
      collectColumnRefs(((Alias) expr).child(), scope, out);
      return;
    }

    // Recurse into sub-expressions (BinaryOp, Cast, ScalarSubquery, etc.)
    List<Expression> children = CollectionConverters.SeqHasAsJava(expr.children()).asJava();
    for (Expression child : children) {
      collectColumnRefs(child, scope, out);
    }
  }

  /**
   * Resolves a column key (e.g. {@code "t.col"} or {@code "col"}) against the
   * current scope, returning the physical {@link ColumnRef}s.
   *
   * <p>Lookup order:
   * <ol>
   *   <li>Exact match in scope (e.g. {@code "alias.col"}).</li>
   *   <li>Unqualified name match across all scope entries.</li>
   *   <li>Fall back to constructing a new {@code ColumnRef} from the name
   *       parts and the first table found in scope.</li>
   * </ol>
   */
  private static List<ColumnRef> resolveColumnKey(String colKey,
      List<String> nameParts,
      Map<String, List<ColumnRef>> scope) {

    // 1. Exact match
    if (scope.containsKey(colKey)) {
      return scope.get(colKey);
    }

    // 2. Two-part: "alias.col" → look for alias in scope, then col
    if (nameParts.size() >= 2) {
      String qualifier = String.join(".", nameParts.subList(0, nameParts.size() - 1));
      String colName   = nameParts.get(nameParts.size() - 1);
      // Try qualified key
      if (scope.containsKey(qualifier + "." + colName)) {
        return scope.get(qualifier + "." + colName);
      }
      // Look for a table entry matching qualifier
      String tableKey = "__table__:" + qualifier;
      if (scope.containsKey(tableKey)) {
        return Collections.singletonList(new ColumnRef(qualifier, colName));
      }
    }

    // 3. Unqualified: scan scope for any entry whose column part matches
    String unqualified = nameParts.get(nameParts.size() - 1);
    for (Map.Entry<String, List<ColumnRef>> e : scope.entrySet()) {
      if (e.getKey().equals(unqualified)
          || e.getKey().endsWith("." + unqualified)) {
        return e.getValue();
      }
    }

    // 4. Last resort: infer table from the first __table__ entry in scope
    String inferredTable = inferTableFromScope(scope);
    return Collections.singletonList(new ColumnRef(inferredTable, unqualified));
  }

  /** Returns the table name from the first {@code __table__:} scope entry. */
  private static String inferTableFromScope(Map<String, List<ColumnRef>> scope) {
    for (String key : scope.keySet()) {
      if (key.startsWith("__table__:")) {
        return key.substring("__table__:".length());
      }
    }
    return ColumnRef.UNKNOWN_TABLE;
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Classification helpers
  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Classifies an output expression as DIRECT, DERIVED, or CONSTANT based on
   * its structure.
   */
  private static TransformType classifyExpression(Expression expr,
      List<ColumnRef> sources) {
    if (sources.isEmpty()) {
      return TransformType.CONSTANT;
    }
    // Peel off the Alias wrapper
    Expression inner = (expr instanceof Alias) ? ((Alias) expr).child() : expr;
    if (inner instanceof UnresolvedAttribute || inner instanceof AttributeReference) {
      return TransformType.DIRECT;
    }
    return TransformType.DERIVED;
  }

  /** Returns {@code true} if {@code expr} or any descendant is an aggregate function. */
  private static boolean containsAggFunction(Expression expr) {
    String className = expr.getClass().getSimpleName();
    // Spark aggregate functions extend AggregateFunction / DeclarativeAggregate
    if (className.startsWith("Count") || className.startsWith("Sum")
        || className.startsWith("Avg") || className.startsWith("Max")
        || className.startsWith("Min") || className.startsWith("First")
        || className.startsWith("Last") || className.startsWith("Collect")
        || className.startsWith("Stddev") || className.startsWith("Variance")
        || className.startsWith("Corr") || className.startsWith("Percentile")
        || isAggFunctionClass(expr)) {
      return true;
    }
    // Recurse
    for (Expression child : CollectionConverters.SeqHasAsJava(expr.children()).asJava()) {
      if (containsAggFunction(child)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isAggFunctionClass(Expression expr) {
    Class<?> c = expr.getClass();
    while (c != null && c != Object.class) {
      String name = c.getSimpleName();
      if (name.equals("AggregateFunction")
          || name.equals("DeclarativeAggregate")
          || name.equals("ImperativeAggregate")
          || name.equals("TypedAggregateExpression")) {
        return true;
      }
      c = c.getSuperclass();
    }
    return false;
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Building the final LineageResult
  // ───────────────────────────────────────────────────────────────────────────

  private static List<FieldLineage> buildLineages(
      Map<String, List<ColumnRef>> scope,
      Map<String, TransformType> typeMap,
      String outputTable) {

    List<FieldLineage> lineages = new ArrayList<>();

    // Filter out internal helper entries
    for (Map.Entry<String, List<ColumnRef>> e : scope.entrySet()) {
      String key = e.getKey();
      if (key.startsWith("__table__:") || key.contains("." + ColumnRef.STAR)) {
        continue;
      }
      // key is the output column name (possibly "alias.col" from subquery)
      String outCol = key.contains(".") ? key.substring(key.lastIndexOf('.') + 1) : key;
      ColumnRef target = new ColumnRef(outputTable, outCol);
      List<ColumnRef> sources = e.getValue();
      // Use the stored TransformType, default to DIRECT/CONSTANT
      TransformType tt = typeMap.getOrDefault(key,
          sources.isEmpty() ? TransformType.CONSTANT : TransformType.DIRECT);
      lineages.add(new FieldLineage(target, sources, tt));
    }

    return lineages;
  }

  /** De-duplicates a list of column references. */
  private static List<ColumnRef> deduplicate(List<ColumnRef> sources) {
    if (sources.isEmpty()) {
      return Collections.emptyList();
    }
    List<ColumnRef> result = new ArrayList<>();
    for (ColumnRef r : sources) {
      if (!result.contains(r)) {
        result.add(r);
      }
    }
    return result;
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Utility helpers
  // ───────────────────────────────────────────────────────────────────────────

  /** Tries to extract the target table name from a DML plan via reflection. */
  private static String extractDmlTarget(LogicalPlan plan) {
    try {
      // InsertIntoStatement.table() returns a LogicalPlan (UnresolvedRelation)
      Object table = plan.getClass().getMethod("table").invoke(plan);
      if (table instanceof LogicalPlan) {
        // UnresolvedRelation → multipartIdentifier
        scala.collection.Seq<?> parts = (scala.collection.Seq<?>)
            table.getClass().getMethod("multipartIdentifier").invoke(table);
        List<?> partList = CollectionConverters.SeqHasAsJava(parts).asJava();
        if (!partList.isEmpty()) {
          return partList.get(partList.size() - 1).toString();
        }
      }
    } catch (Exception e) {
      LOG.debug("extractDmlTarget: {}", e.getMessage());
    }
    return null;
  }

  /** Returns the first (query) child of a DML plan. */
  private static LogicalPlan firstChild(LogicalPlan plan) {
    List<LogicalPlan> children =
        CollectionConverters.SeqHasAsJava(plan.children()).asJava();
    if (!children.isEmpty()) {
      return children.get(0);
    }
    return plan;
  }
}
