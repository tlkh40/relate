package net.lecousin.reactive.data.relational.configuration;

import io.r2dbc.spi.ConnectionFactory;
import net.lecousin.reactive.data.relational.LcReactiveDataRelationalClient;
import net.lecousin.reactive.data.relational.mapping.LcMappingR2dbcConverter;
import net.lecousin.reactive.data.relational.mapping.LcReactiveDataAccessStrategy;
import net.lecousin.reactive.data.relational.repository.LcR2dbcEntityTemplate;
import net.lecousin.reactive.data.relational.schema.dialect.RelationalDatabaseSchemaDialect;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.util.Assert;

/**
 * Configure R2DBC spring data extended by lc-reactive-spring-data-relational.
 */
@Configuration
public abstract class LcReactiveDataRelationalConfiguration extends AbstractR2dbcConfiguration {

    private static final String CONNECTION_FACTORY_BEAN_NAME = "connectionFactory";

    @Nullable
    protected ApplicationContext context;

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) {
        this.context = applicationContext;
        super.setApplicationContext(applicationContext);
    }

    public abstract RelationalDatabaseSchemaDialect schemaDialect();

    @SuppressWarnings("unchecked")
    public LcReactiveDataRelationalClient getLcClient(DatabaseClient databaseClient, ReactiveDataAccessStrategy dataAccessStrategy) {
        return new LcReactiveDataRelationalClient(
                databaseClient,
                (MappingContext<
                        RelationalPersistentEntity<?>,
                        ? extends RelationalPersistentProperty>)
                        dataAccessStrategy.getConverter().getMappingContext(),
                schemaDialect(),
                (LcReactiveDataAccessStrategy) dataAccessStrategy,
                (LcMappingR2dbcConverter) dataAccessStrategy.getConverter());
    }

    @Bean
    @Override
    public @NonNull
    LcReactiveDataAccessStrategy reactiveDataAccessStrategy(@NonNull R2dbcConverter converter) {
        return new LcReactiveDataAccessStrategy(getDialect(getConnectionFactory()), (LcMappingR2dbcConverter) converter);
    }

    @Override
    public @NonNull
    MappingR2dbcConverter r2dbcConverter(
            @NonNull R2dbcMappingContext mappingContext,
            @NonNull R2dbcCustomConversions r2dbcCustomConversions
    ) {
        return new LcMappingR2dbcConverter(mappingContext, r2dbcCustomConversions);
    }

    @Bean
    @Override
    public @NonNull
    R2dbcEntityTemplate r2dbcEntityTemplate(
            @NonNull DatabaseClient databaseClient,
            @NonNull ReactiveDataAccessStrategy dataAccessStrategy
    ) {
        return new LcR2dbcEntityTemplate(getLcClient(databaseClient, dataAccessStrategy));
    }

    @Override
    public @NonNull
    ConnectionFactory connectionFactory() {
        //noinspection ConstantConditions
        return null;
    }

    @SuppressWarnings("java:S112")
    private ConnectionFactory getConnectionFactory() {
        Assert.notNull(context, "ApplicationContext is not yet initialized");

        String[] beanNamesForType = context.getBeanNamesForType(ConnectionFactory.class);

        for (String beanName : beanNamesForType) {

            if (beanName.equals(CONNECTION_FACTORY_BEAN_NAME)) {
                return context.getBean(CONNECTION_FACTORY_BEAN_NAME, ConnectionFactory.class);
            }
        }

        ConnectionFactory factory = connectionFactory();
        //noinspection ConstantConditions
        if (factory == null) {
            throw new RuntimeException("No r2dbc connection factory defined");
        }
        return factory;
    }
}
