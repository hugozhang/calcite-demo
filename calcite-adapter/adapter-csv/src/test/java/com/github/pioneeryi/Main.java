package com.github.pioneeryi;

import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.*;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: hugo.zxh
 * @date: 2023/07/09 18:26
 */
public class Main {

    public static void main(String[] args) throws SqlParseException, SQLException {
        SqlParser parser = SqlParser.create("select * from DEPTS where DEPTNO>=20 ", SqlParser.Config.DEFAULT);
        // 解析SQL字符串, 生成SqlNode树
        SqlNode sqlNode = parser.parseStmt();
        System.out.println(sqlNode);
        CsvSchema csvSchema = new CsvSchema();
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true, false);
        rootSchema.add("test_schema", csvSchema);
        SchemaPlus schemaPlus = Frameworks.createRootSchema(false);
        schemaPlus.add("test_schema", csvSchema);
        CalciteConnectionConfigImpl config = new CalciteConnectionConfigImpl(new Properties());
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(rootSchema, Collections.singletonList("test_schema"), typeFactory, config);
        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withSqlConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);
        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader, typeFactory, validatorConfig);
        // 执行SQL验证
        SqlNode validateSqlNode = validator.validate(sqlNode);
        System.out.println(sqlNode);
        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        // 创建SqlToRelConverter
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config().withTrimUnusedFields(true).withExpand(false);
        SqlToRelConverter converter = new SqlToRelConverter(null, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, converterConfig);
        // 将SqlNode树转化为RelNode树
        RelRoot relNode = converter.convertQuery(validateSqlNode, false, true);
        System.out.println(relNode);

        // 规则
        RuleSet rules = RuleSets.ofList(CoreRules.FILTER_TO_CALC, CoreRules.PROJECT_TO_CALC, CoreRules.FILTER_CALC_MERGE, CoreRules.PROJECT_CALC_MERGE, CoreRules.FILTER_INTO_JOIN,  // 过滤谓词下推到Join之前
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE, EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE, EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE, EnumerableRules.ENUMERABLE_JOIN_RULE, EnumerableRules.ENUMERABLE_SORT_RULE, EnumerableRules.ENUMERABLE_CALC_RULE, EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        Program program = Programs.of(RuleSets.ofList(rules));
        RelNode optimizerRelTree = program.run(planner, relNode.rel, relNode.rel.getTraitSet().plus(EnumerableConvention.INSTANCE), Collections.emptyList(), Collections.emptyList());
        System.out.println(optimizerRelTree);


        // 生成物理执行计划
        EnumerableRel enumerable = (EnumerableRel) optimizerRelTree;
        Map<String, Object> internalParameters = new LinkedHashMap<>();
        EnumerableRel.Prefer prefer = EnumerableRel.Prefer.ARRAY;
        Bindable bindable = EnumerableInterpretable.toBindable(internalParameters, null, enumerable, prefer);

        // 1.以驱动的形式调用
//        final Properties properties = new Properties();
//        properties.put("caseSensitive", "true");
//        Connection connection = DriverManager.getConnection("jdbc:calcite:", properties);
//        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
//        DataContext dataContext = DataContexts.of(calciteConnection, schemaPlus);
//        Enumerable bind = bindable.bind(dataContext);

        //2. 非驱动的形式
        Enumerable bind = bindable.bind(new DataContext() {
            @Override
            public @Nullable SchemaPlus getRootSchema() {
                return schemaPlus;
            }

            @Override
            public JavaTypeFactory getTypeFactory() {
                return new JavaTypeFactoryImpl();
            }

            @Override
            public QueryProvider getQueryProvider() {
                return null;
            }

            @Override
            public @Nullable Object get(String s) {
                return null;
            }
        });

        Enumerator enumerator = bind.enumerator();
        while (enumerator.moveNext()) {
            Object current = enumerator.current();
            Object[] values = (Object[]) current;
            StringBuilder sb = new StringBuilder();
            for (Object v : values) {
                sb.append(v).append(",");
            }
            sb.setLength(sb.length() - 1);
            System.out.println(sb);
        }
    }

}
