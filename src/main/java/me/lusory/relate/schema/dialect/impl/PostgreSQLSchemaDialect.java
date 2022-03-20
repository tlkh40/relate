package me.lusory.relate.schema.dialect.impl;

import io.r2dbc.postgresql.api.PostgresqlConnection;
import me.lusory.relate.annotations.ColumnDefinition;
import me.lusory.relate.schema.Column;
import me.lusory.relate.schema.dialect.RelationalDatabaseSchemaDialect;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Expressions;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.SimpleFunction;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;

@Component
@ConditionalOnClass(PostgresqlConnection.class)
public class PostgreSQLSchemaDialect extends RelationalDatabaseSchemaDialect {
    private static final String EXTRACT_DATE_TIME_FUNCTION = "EXTRACT";

    @Override
    public String getName() {
        return "PostgreSQL";
    }

    @Override
    public boolean isCompatible(R2dbcDialect r2dbcDialect) {
        return r2dbcDialect.getClass().equals(PostgresDialect.class);
    }

    @Override
    protected void addAutoIncrement(Column col, StringBuilder sql) {
        // nothing to add
    }

    @Override
    protected String getColumnTypeByte(Column col, Class<?> type, ColumnDefinition def) {
        return getColumnTypeShort(col, type, def);
    }

    @Override
    protected String getColumnTypeShort(Column col, Class<?> type, ColumnDefinition def) {
        if (col.isAutoIncrement()) {
            return "SMALLSERIAL";
        }
        return "SMALLINT";
    }

    @Override
    protected String getColumnTypeInteger(Column col, Class<?> type, ColumnDefinition def) {
        if (col.isAutoIncrement()) {
            return "SERIAL";
        }
        return "INTEGER";
    }

    @Override
    protected String getColumnTypeLong(Column col, Class<?> type, ColumnDefinition def) {
        if (col.isAutoIncrement()) {
            return "BIGSERIAL";
        }
        return "BIGINT";
    }

    @Override
    protected String getColumnTypeFloat(Column col, Class<?> type, ColumnDefinition def) {
        return "REAL";
    }

    @Override
    protected String getColumnTypeDouble(Column col, Class<?> type, ColumnDefinition def) {
        return "DOUBLE PRECISION";
    }

    @Override
    protected String getColumnTypeDateTime(Column col, Class<?> type, ColumnDefinition def) {
        int precision = def != null ? def.precision() : -1;
        if (precision < 0) {
            precision = DEFAULT_TIME_PRECISION;
        }
        return "TIMESTAMP(" + precision + ")";
    }

    @Override
    protected String getColumnTypeDateTimeWithTimeZone(
            Column col, Class<?> type, ColumnDefinition def) {
        int precision = def != null ? def.precision() : -1;
        if (precision < 0) {
            precision = DEFAULT_TIME_PRECISION;
        }
        return "TIMESTAMP(" + precision + ") WITH TIME ZONE";
    }

    @Override
    protected String getColumnTypeString(Column col, Class<?> type, ColumnDefinition def) {
        if (def != null) {
            if (def.max() > Integer.MAX_VALUE) {
                // large text
                return "CLOB(" + def.max() + ")";
            }
            if (def.min() > 0 && def.max() == def.min()) {
                // fixed length
                return "CHAR(" + def.max() + ")";
            }
            if (def.max() > 0) {
                // max length
                return "VARCHAR(" + def.max() + ")";
            }
        }
        return "VARCHAR";
    }

    @Override
    protected void addDefaultRandomUuid(Column col, StringBuilder sql) {
        sql.append(" DEFAULT UUID_GENERATE_V4()");
    }

    @Override
    public Expression applyFunctionTo(SqlFunction function, Expression expression) {
        switch (function) {
            case YEAR:
                return SimpleFunction.create(
                        EXTRACT_DATE_TIME_FUNCTION,
                        Collections.singletonList(Expressions.just("YEAR FROM " + expression)));
            case MONTH:
                return SimpleFunction.create(
                        EXTRACT_DATE_TIME_FUNCTION,
                        Collections.singletonList(Expressions.just("MONTH FROM " + expression)));
            case DAY_OF_MONTH:
                return SimpleFunction.create(
                        EXTRACT_DATE_TIME_FUNCTION,
                        Collections.singletonList(Expressions.just("DAY FROM " + expression)));
            case DAY_OF_YEAR:
                return SimpleFunction.create(
                        EXTRACT_DATE_TIME_FUNCTION,
                        Collections.singletonList(Expressions.just("DOY FROM " + expression)));
            case HOUR:
                return SimpleFunction.create(
                        EXTRACT_DATE_TIME_FUNCTION,
                        Collections.singletonList(Expressions.just("HOUR FROM " + expression)));
            case MINUTE:
                return SimpleFunction.create(
                        EXTRACT_DATE_TIME_FUNCTION,
                        Collections.singletonList(Expressions.just("MINUTE FROM " + expression)));
            case ISO_WEEK:
                return SimpleFunction.create(
                        EXTRACT_DATE_TIME_FUNCTION,
                        Collections.singletonList(Expressions.just("WEEK FROM " + expression)));
            case ISO_DAY_OF_WEEK:
                return SimpleFunction.create(
                        EXTRACT_DATE_TIME_FUNCTION,
                        Collections.singletonList(Expressions.just("ISODOW FROM " + expression)));
            case SECOND:
                return SimpleFunction.create(
                        EXTRACT_DATE_TIME_FUNCTION,
                        Collections.singletonList(
                                Expressions.just(
                                        "SECOND FROM "
                                                + SimpleFunction.create(
                                                "DATE_TRUNC",
                                                Arrays.asList(
                                                        SQL.literalOf("second"),
                                                        expression)))));
            default:
                break;
        }
        return super.applyFunctionTo(function, expression);
    }
}
