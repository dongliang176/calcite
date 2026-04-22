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
package org.apache.calcite.lineageparse.calcite;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.lineageparse.model.ColumnRef;
import org.apache.calcite.lineageparse.model.FieldLineage;
import org.apache.calcite.lineageparse.model.FieldLineage.TransformType;
import org.apache.calcite.lineageparse.model.LineageResult;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Extracts <strong>field-level lineage</strong> from standard SQL statements
 * using the <em>Apache Calcite</em> planner and
 * {@link RelMetadataQuery#getColumnOrigins(RelNode, int)}.
 *
 * <h2>Design</h2>
 * <p>Calcite provides built-in column-origin metadata via
 * {@link org.apache.calcite.rel.metadata.RelMdColumnOrigins}.  After parsing
 * and converting a SQL string to a {@link RelNode} tree, we iterate over each
 * output field and call {@code getColumnOrigins} to obtain the set of physical
 * table columns that contributed to it.
 *
 * <p>Because column-origin tracking requires a real schema (so the planner
 * knows which columns exist on each table), callers must supply a
 * {@link SchemaPlus} that registers all referenced tables.  A convenience
 * factory method {@link #buildSchema(Map)} is provided so that callers can
 * supply a simple {@code Map<tableName, List<columnName>>} instead.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // 1. Define the schema
 * Map<String, List<String>> schema = new LinkedHashMap<>();
 * schema.put("orders",   Arrays.asList("id", "customer_id", "price", "qty"));
 * schema.put("customers", Arrays.asList("id", "name", "email"));
 *
 * SchemaPlus rootSchema = CalciteLineageExtractor.buildSchema(schema);
 *
 * // 2. Extract lineage
 * String sql = "SELECT o.id, o.price * o.qty AS total, c.name "
 *            + "FROM orders o JOIN customers c ON o.customer_id = c.id";
 * LineageResult result = CalciteLineageExtractor.extract(sql, rootSchema, "result");
 *
 * result.getFieldLineages().forEach(System.out::println);
 * }</pre>
 */
public final class CalciteLineageExtractor {

  private static final Logger LOG =
      LoggerFactory.getLogger(CalciteLineageExtractor.class);

  private CalciteLineageExtractor() {
    // utility class
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Public API
  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Parses and analyses {@code sql} using the provided {@code rootSchema}, then
   * returns field-level lineage for every output column.
   *
   * @param sql         standard SQL SELECT (or INSERT INTO … SELECT)
   * @param rootSchema  Calcite schema containing all referenced tables
   * @param outputTable name to assign to the output table in the result
   * @return            field-level lineage result
   * @throws Exception  if parsing or plan conversion fails
   */
  public static LineageResult extract(String sql, SchemaPlus rootSchema,
      String outputTable) throws Exception {

    // ── 1. Build framework config ──────────────────────────────────────────
    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(rootSchema)
        .parserConfig(SqlParser.config()
            .withCaseSensitive(false))
        .operatorTable(SqlStdOperatorTable.instance())
        .build();

    // ── 2. Parse SQL ──────────────────────────────────────────────────────
    SqlParser parser = SqlParser.create(sql, config.getParserConfig());
    SqlNode sqlNode = parser.parseQuery();
    LOG.debug("Parsed SQL node:\n{}", sqlNode);

    // ── 3. Validate & convert to RelNode ──────────────────────────────────
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig connCfg = new CalciteConnectionConfigImpl(props);

    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    RelDataTypeFactory typeFactory = config.getTypeSystem() != null
        ? new org.apache.calcite.sql.type.SqlTypeFactoryImpl(config.getTypeSystem())
        : new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
              org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

    CalciteCatalogReader catalogReader = new CalciteCatalogReader(
        calciteSchema,
        Collections.singletonList(""),
        typeFactory,
        connCfg);

    SqlValidator validator = SqlValidatorUtil.newValidator(
        SqlStdOperatorTable.instance(),
        catalogReader,
        typeFactory,
        SqlValidator.Config.DEFAULT.withLenientOperatorLookup(true));

    SqlNode validatedNode = validator.validate(sqlNode);

    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(
        org.apache.calcite.plan.ConventionTraitDef.INSTANCE);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

    SqlToRelConverter converter = new SqlToRelConverter(
        (rowType, queryString, schemaPath, viewExpander) -> null,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());

    RelRoot relRoot = converter.convertQuery(validatedNode, false, true);
    RelNode relNode = relRoot.rel;
    LOG.debug("RelNode plan:\n{}", relNode.explain());

    // ── 4. Extract column origins via RelMetadataQuery ─────────────────────
    return buildLineageFromRelNode(sql, relNode, outputTable);
  }

  /**
   * Convenience overload – uses {@code "result"} as the output table name.
   */
  public static LineageResult extract(String sql, SchemaPlus rootSchema)
      throws Exception {
    return extract(sql, rootSchema, "result");
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Schema builder utility
  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Builds a Calcite {@link SchemaPlus} from a simple column map.
   *
   * <p>Each entry in {@code tableColumns} is {@code tableName → list of column names}.
   * All columns are typed as {@code VARCHAR} for simplicity; adjust the
   * implementation for richer type information.
   *
   * @param tableColumns map from table name to its ordered list of column names
   * @return Calcite root schema suitable for use with {@link #extract}
   */
  public static SchemaPlus buildSchema(Map<String, List<String>> tableColumns) {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    for (Map.Entry<String, List<String>> entry : tableColumns.entrySet()) {
      final String tableName = entry.getKey();
      final List<String> columns = entry.getValue();
      rootSchema.add(tableName, buildTable(tableName, columns));
    }
    return rootSchema;
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Internal helpers
  // ───────────────────────────────────────────────────────────────────────────

  private static LineageResult buildLineageFromRelNode(String sql, RelNode root,
      String outputTable) {

    RelMetadataQuery mq = root.getCluster().getMetadataQuery();
    RelDataType rowType = root.getRowType();
    List<RelDataTypeField> fields = rowType.getFieldList();

    List<FieldLineage> lineages = new ArrayList<>();
    for (int i = 0; i < fields.size(); i++) {
      String outColName = fields.get(i).getName();
      ColumnRef target = new ColumnRef(outputTable, outColName);

      Set<RelColumnOrigin> origins;
      try {
        origins = mq.getColumnOrigins(root, i);
      } catch (Exception e) {
        LOG.warn("getColumnOrigins failed for column {}: {}", outColName, e.getMessage());
        origins = null;
      }

      List<ColumnRef> sources = new ArrayList<>();
      boolean isDerived = false;

      if (origins != null) {
        for (RelColumnOrigin origin : origins) {
          RelOptTable originTable = origin.getOriginTable();
          List<String> qualifiedName = originTable.getQualifiedName();
          String tableName = qualifiedName.get(qualifiedName.size() - 1);
          String database  = qualifiedName.size() > 1
              ? String.join(".", qualifiedName.subList(0, qualifiedName.size() - 1))
              : null;
          int colOrd = origin.getOriginColumnOrdinal();
          String originColName = originTable.getRowType()
              .getFieldList().get(colOrd).getName();
          sources.add(new ColumnRef(database, tableName, originColName));
          if (origin.isDerived()) {
            isDerived = true;
          }
        }
      }

      TransformType tt = resolveTransformType(sources, isDerived);
      lineages.add(new FieldLineage(target, sources, tt));
    }

    return new LineageResult(sql, lineages, null);
  }

  private static TransformType resolveTransformType(List<ColumnRef> sources,
      boolean isDerived) {
    if (sources.isEmpty()) {
      return TransformType.CONSTANT;
    }
    return isDerived ? TransformType.DERIVED : TransformType.DIRECT;
  }

  /** Creates a simple DOUBLE-typed table (numeric columns support aggregate functions). */
  private static Table buildTable(String tableName, List<String> columns) {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        org.apache.calcite.rel.type.RelDataTypeFactory.Builder builder =
            typeFactory.builder();
        for (String col : columns) {
          builder.add(col,
              typeFactory.createSqlType(
                  org.apache.calcite.sql.type.SqlTypeName.DOUBLE));
        }
        return builder.build();
      }
    };
  }
}
