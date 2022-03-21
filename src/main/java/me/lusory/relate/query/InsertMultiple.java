package me.lusory.relate.query;

import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.core.sql.render.NamingStrategies;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.RenderNamingStrategy;
import org.springframework.data.relational.core.sql.render.SelectRenderContext;
import org.springframework.lang.NonNull;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Specify an INSERT query, with multiple rows.<br>
 * As Spring Data R2DBC does not support it, we define a custom request, but it cannot be used with
 * MySql which does not support to return all generated values.<br>
 * An InsertMultiple can be used in a SqlQuery to be executed.
 */
public class InsertMultiple {

    private final Table into;
    private final List<Column> columns;
    private final List<InsertRowValues> values;

    public InsertMultiple(Table into, List<Column> columns, List<List<Expression>> values) {
        this.into = into;
        this.columns = new ArrayList<>(columns);
        this.values = new ArrayList<>(values.size());
        for (List<Expression> list : values) {
            this.values.add(new InsertRowValues(new ArrayList<>(list)));
        }
    }

    private static String render(SqlIdentifier identifier, RenderContext renderContext) {
        return identifier.toSql(renderContext.getIdentifierProcessing());
    }

    private static String render(Expression expression, RenderContext renderContext) {
        if (expression instanceof Named) {
            return render(((Named) expression).getName(), renderContext);
        }
        return expression.toString();
    }

    public String render(RenderContext renderContext) {
        if (renderContext == null) {
            renderContext = new SimpleRenderContext();
        }
        StringBuilder sql = new StringBuilder(512 + 16 * values.size());
        sql.append("INSERT INTO ");
        sql.append(render(renderContext.getNamingStrategy().getName(into), renderContext));
        sql.append(" (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append(render(renderContext.getNamingStrategy().getName(columns.get(i)), renderContext));
        }
        sql.append(") VALUES ");
        for (int row = 0; row < values.size(); row++) {
            if (row > 0) {
                sql.append(',');
            }
            sql.append('(');
            InsertRowValues rowValues = values.get(row);
            for (int i = 0; i < rowValues.expressions.size(); i++) {
                if (i > 0) {
                    sql.append(',');
                }
                sql.append(render(rowValues.expressions.get(i), renderContext));
            }
            sql.append(')');
        }
        return sql.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append("INSERT INTO ").append(this.into);

        if (!this.columns.isEmpty()) {
            builder.append(" (")
                    .append(
                            StringUtils.collectionToDelimitedString(
                                    this.columns.stream()
                                            .map(col -> col.getName().toString())
                                            .collect(Collectors.toList()),
                                    ","))
                    .append(")");
        }

        if (!this.values.isEmpty()) {
            builder.append(" VALUES ");
            builder.append(StringUtils.collectionToDelimitedString(this.values, ","));
        }

        return builder.toString();
    }

    static class SimpleRenderContext implements RenderContext {

        private final RenderNamingStrategy namingStrategy;

        SimpleRenderContext() {
            this.namingStrategy = NamingStrategies.asIs();
        }

        @Override
        public @NonNull
        IdentifierProcessing getIdentifierProcessing() {
            return IdentifierProcessing.NONE;
        }

        @Override
        public @NonNull
        SelectRenderContext getSelect() {
            return DefaultSelectRenderContext.INSTANCE;
        }

        @Override
        public @NonNull
        RenderNamingStrategy getNamingStrategy() {
            return this.namingStrategy;
        }

        enum DefaultSelectRenderContext implements SelectRenderContext {
            INSTANCE
        }
    }

    public static class InsertRowValues {

        private final List<Expression> expressions;

        InsertRowValues(List<Expression> expressions) {
            this.expressions = expressions;
        }

        @Override
        public String toString() {
            return "(" + StringUtils.collectionToDelimitedString(this.expressions, ",") + ")";
        }
    }
}
