package me.lusory.relate.mapping;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import me.lusory.relate.model.PropertiesSourceRow;
import me.lusory.relate.LcReactiveDataRelationalClient;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

public class LcMappingR2dbcConverter extends MappingR2dbcConverter implements R2dbcConverter {

    private LcReactiveDataRelationalClient client;

    public LcMappingR2dbcConverter(
            MappingContext<
                    ? extends RelationalPersistentEntity<?>,
                    ? extends RelationalPersistentProperty>
                    context,
            CustomConversions conversions) {
        super(context, conversions);
    }

    public LcReactiveDataRelationalClient getLcClient() {
        return client;
    }

    public void setLcClient(LcReactiveDataRelationalClient client) {
        this.client = client;
    }

    @Override
    public @NonNull
    <R> R read(@NonNull Class<R> type, @NonNull Row row, @Nullable RowMetadata metadata) {
        return new LcEntityReader(null, null, client)
                .read(type, new PropertiesSourceRow(row, metadata));
    }

    @Override
    public Object readValue(@Nullable Object value, @NonNull TypeInformation<?> type) {
        return new LcEntityReader(null, null, client).readValue(value, type);
    }

    // ----------------------------------
    // Entity writing
    // ----------------------------------

    /*
     * (non-Javadoc)
     * @see org.springframework.data.convert.EntityWriter#write(java.lang.Object, java.lang.Object)
     */
    @Override
    public void write(@NonNull Object source, @NonNull OutboundRow sink) {
        new LcEntityWriter(this).write(source, sink);
    }
}
