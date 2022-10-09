import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.*;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES;

public class Application {

    private static SqlParser.Config sqlParserConfig = SqlParser.config()
                    .withCaseSensitive(false)
                    .withQuotedCasing(Casing.TO_UPPER)
                    .withUnquotedCasing(Casing.TO_UPPER);

    private static SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    private static RelDataTypeSystem typeSystem = RelDataTypeSystem.DEFAULT;

    private static Planner planner;

    public static void main(String[] args) {
        String sql =
                "SELECT u.id, name, age, sum(price) " + "FROM users AS u join orders AS o ON u.id = o.user_id " +
                        "WHERE age >= 20 AND age <= 30 " + "GROUP BY u.id, name, age " + "ORDER BY u.id";

        planner = Frameworks.getPlanner(getFrameworkConfig());

        // 将 SQL 转换为 SQLNode
        SqlNode originSqlNode = sqlParse(sql);

        // 对 SQL 进行校验
        SqlNode validatedSqlNode = validateSql(originSqlNode);

        // 转换为关系代数 RelNode
        RelRoot relRoot = toRelNode(validatedSqlNode);
        System.out.println(RelOptUtil.toString(relRoot.rel, ALL_ATTRIBUTES));

        // 对查询进行优化
        //RelNode optimizedRelNode = optimize(relRoot.rel);

        // 执行
        execute(relRoot.rel);
    }

    private static SqlNode sqlParse(String sql) {
        try {
            return planner.parse(sql);
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static SqlNode validateSql(SqlNode sqlNode) {
        try {
            // 执行SQL验证
            return planner.validate(sqlNode);
        } catch (ValidationException e) {
            throw new RuntimeException(e);
        }
    }

    private static RelRoot toRelNode(SqlNode sqlNode) {
        try {
            // 执行SQL验证
            return planner.rel(sqlNode);
        } catch (RelConversionException e) {
            throw new RuntimeException(e);
        }
    }

    private static RelNode optimize(RelNode relNode) {
        HepProgramBuilder builder = new HepProgramBuilder();
        HepPlanner hepPlanner = new HepPlanner(builder.build());

        // 优化规则
        RuleSet rules = RuleSets.ofList(CoreRules.FILTER_TO_CALC, CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE, CoreRules.PROJECT_CALC_MERGE, CoreRules.FILTER_INTO_JOIN,     //
                // 过滤谓词下推到Join之前
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE, EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE, EnumerableRules.ENUMERABLE_JOIN_RULE,
                EnumerableRules.ENUMERABLE_SORT_RULE, EnumerableRules.ENUMERABLE_CALC_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        Program program = Programs.of(RuleSets.ofList(rules));
        RelNode optimizerRelTree = program.run(hepPlanner, relNode,
                relNode.getTraitSet().plus(EnumerableConvention.INSTANCE), Collections.emptyList(),
                Collections.emptyList());
        return optimizerRelTree;
    }

    private static void execute(RelNode optimizerRelTree) {
        EnumerableRel enumerable = (EnumerableRel) optimizerRelTree;
        Map<String, Object> internalParameters = new LinkedHashMap<>();
        EnumerableRel.Prefer prefer = EnumerableRel.Prefer.ARRAY;
        Bindable bindable = EnumerableInterpretable.toBindable(internalParameters, null, enumerable, prefer);
        Enumerable bind = bindable.bind(new DataContext() {
            @Override
            public @Nullable SchemaPlus getRootSchema() {
                return getCalciteRootSchema().plus();
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

    private static FrameworkConfig getFrameworkConfig() {
        return Frameworks.newConfigBuilder().defaultSchema(getCalciteRootSchema().plus())
                .parserConfig(sqlParserConfig)
                .operatorTable(SqlStdOperatorTable.instance())
                .build();
    }

    private static CalciteSchema getCalciteRootSchema() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add("users", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
                RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
                builder.add("id", new BasicSqlType(typeSystem, SqlTypeName.VARCHAR));
                builder.add("name", new BasicSqlType(typeSystem, SqlTypeName.VARCHAR));
                builder.add("age", new BasicSqlType(typeSystem, SqlTypeName.INTEGER));
                return builder.build();
            }
        });
        rootSchema.add("orders", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
                RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
                builder.add("id", new BasicSqlType(typeSystem, SqlTypeName.VARCHAR));
                builder.add("user_id", new BasicSqlType(typeSystem, SqlTypeName.VARCHAR));
                builder.add("goods", new BasicSqlType(typeSystem, SqlTypeName.VARCHAR));
                builder.add("price", new BasicSqlType(typeSystem, SqlTypeName.DECIMAL));
                return builder.build();
            }
        });
        return rootSchema;
    }
}
