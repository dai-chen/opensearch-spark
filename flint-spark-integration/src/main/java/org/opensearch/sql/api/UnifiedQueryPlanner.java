/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.common.antlr.Parser;
import org.opensearch.sql.executor.OpenSearchTypeSystem;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class UnifiedQueryPlanner {
    private final Parser parser;
    private final CalciteRelNodeVisitor relNodeVisitor = new CalciteRelNodeVisitor();
    private final CalcitePlanContext calcitePlanContext;

    public UnifiedQueryPlanner(QueryType queryType, SchemaPlus rootSchema) {
        if (queryType == QueryType.PPL) {
            parser = new PPLSyntaxParser();
        } else {
            throw new UnsupportedOperationException("Unsupported query type: " + queryType);
        }

        Frameworks.ConfigBuilder configBuilder =
                Frameworks.newConfigBuilder()
                        .parserConfig(SqlParser.Config.DEFAULT) // TODO check
                        .defaultSchema(rootSchema)
                        .traitDefs((List<RelTraitDef>) null)
                        .programs(Programs.calc(DefaultRelMetadataProvider.INSTANCE))
                        .typeSystem(OpenSearchTypeSystem.INSTANCE);

        FrameworkConfig calciteConfig = configBuilder.build();
        calcitePlanContext = CalcitePlanContext.create(calciteConfig, queryType);
    }

    public RelNode plan(String query) {
        ParseTree cst = parser.parse(query);
        Statement statement = cst.accept(
                new AstStatementBuilder(
                        new AstBuilder(query),
                        AstStatementBuilder.StatementBuilderContext.builder()
                                .isExplain(false)
                                .format("jdbc")
                                .build()));
        UnresolvedPlan ast = ((Query) statement).getPlan();
        RelNode logical = relNodeVisitor.analyze(ast, calcitePlanContext);

        RelNode calcitePlan = logical;
        /* Calcite only ensures collation of the final result produced from the root sort operator.
         * While we expect that the collation can be preserved through the pipes over PPL, we need to
         * explicitly add a sort operator on top of the original plan
         * to ensure the correct collation of the final result.
         * See logic in ${@link CalcitePrepareImpl}
         * For the redundant sort, we rely on Calcite optimizer to eliminate
         */
        RelCollation collation = logical.getTraitSet().getCollation();
        if (!(logical instanceof Sort) && collation != RelCollations.EMPTY) {
            calcitePlan = LogicalSort.create(logical, collation, null, null);
        }
        return calcitePlan;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private QueryType queryType;
        private SchemaPlus rootSchema;

        public Builder() {
            this.rootSchema = CalciteSchema.createRootSchema(true, false).plus();
        }

        public Builder language(QueryType queryType) {
            this.queryType = queryType;
            return this;
        }

        public Builder catalog(String catalogName, Map<String, Schema> databases) {
            SchemaPlus catalog = rootSchema.add(catalogName, new AbstractSchema());
            for (Map.Entry<String, Schema> e : databases.entrySet()) {
                catalog.add(e.getKey(), e.getValue());
            }
            return this;
        }

        public Builder catalogs(Consumer<SchemaPlus> catalogPopulator) {
            catalogPopulator.accept(rootSchema);
            return this;
        }

        public UnifiedQueryPlanner build() {
            return new UnifiedQueryPlanner(queryType, rootSchema);
        }
    }
}
