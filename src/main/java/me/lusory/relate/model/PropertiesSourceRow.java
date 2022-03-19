package me.lusory.relate.model;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

public class PropertiesSourceRow implements PropertiesSource {

    private final Row row;
    private final RowMetadata metadata;

    public PropertiesSourceRow(Row row, @Nullable RowMetadata metadata) {
        this.row = row;
        this.metadata = metadata;
    }

    @Override
    public Object getSource() {
        return row;
    }

    @Override
    public boolean isPropertyPresent(RelationalPersistentProperty property) {
        return metadata == null || metadata.contains(property.getColumnName().toString());
    }

    @Override
    public Object getPropertyValue(RelationalPersistentProperty property) {
        return row.get(property.getColumnName().toString());
    }
}
