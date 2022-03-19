package me.lusory.relate.mapping;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import me.lusory.relate.model.PropertiesSourceRow;
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.lang.NonNull;

import java.util.function.BiFunction;

public class LcReactiveDataAccessStrategy extends DefaultReactiveDataAccessStrategy {

    protected R2dbcDialect dialect;

    public LcReactiveDataAccessStrategy(R2dbcDialect dialect, LcMappingR2dbcConverter converter) {
        super(dialect, converter);
        this.dialect = dialect;
    }

    @Override
    public @NonNull
    <T> BiFunction<Row, RowMetadata, T> getRowMapper(@NonNull Class<T> typeToRead) {
        LcEntityReader reader = new LcEntityReader(null, (LcMappingR2dbcConverter) getConverter());
        return (row, metadata) -> reader.read(typeToRead, new PropertiesSourceRow(row, metadata));
    }

    public R2dbcDialect getDialect() {
        return dialect;
    }
}
