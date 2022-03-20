package me.lusory.relate.repository;

import me.lusory.relate.model.ModelUtils;
import me.lusory.relate.query.criteria.Criteria;
import me.lusory.relate.LcReactiveDataRelationalClient;
import me.lusory.relate.query.SelectQuery;
import me.lusory.relate.query.SqlQuery;
import org.reactivestreams.Publisher;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.repository.support.SimpleR2dbcRepository;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.util.Streamable;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;

@SuppressWarnings("java:S119") // name of parameter ID
public class LcR2dbcRepositoryImpl<T, ID> extends SimpleR2dbcRepository<T, ID>
        implements LcR2dbcRepository<T, ID> {

    private final LcReactiveDataRelationalClient lcClient;
    private final RelationalEntityInformation<T, ID> entityInfo;
    private final R2dbcEntityOperations entityOperations;

    public LcR2dbcRepositoryImpl(
            RelationalEntityInformation<T, ID> entity,
            R2dbcEntityOperations entityOperations,
            R2dbcConverter converter) {
        super(entity, entityOperations, converter);
        lcClient = ((LcR2dbcEntityTemplate) entityOperations).getLcClient();
        this.entityInfo = entity;
        this.entityOperations = entityOperations;
    }

    @Override
    public LcReactiveDataRelationalClient getLcClient() {
        return lcClient;
    }

    @Override
    public @NonNull
    Mono<T> findById(@NonNull ID id) {
        Assert.notNull(id, "Id must not be null in findById");
        RelationalPersistentEntity<?> entity =
                lcClient.getMappingContext().getRequiredPersistentEntity(entityInfo.getJavaType());
        RelationalPersistentProperty idProperty = entity.getRequiredIdProperty();
        return SelectQuery.from(entityInfo.getJavaType(), "e")
                .where(Criteria.property("e", idProperty.getName()).is(id))
                .execute(lcClient)
                .next();
    }

    @Override
    public @NonNull
    Flux<T> findAllById(@NonNull Publisher<ID> idPublisher) {
        RelationalPersistentEntity<?> entity =
                lcClient.getMappingContext().getRequiredPersistentEntity(entityInfo.getJavaType());
        RelationalPersistentProperty idProperty = entity.getRequiredIdProperty();
        return Flux.from(idPublisher)
                .buffer()
                .filter(ids -> !ids.isEmpty())
                .concatMap(
                        ids -> {
                            if (ids.isEmpty()) {
                                return Flux.empty();
                            }
                            return SelectQuery.from(entityInfo.getJavaType(), "e")
                                    .where(Criteria.property("e", idProperty.getName()).in(ids))
                                    .execute(lcClient);
                        });
    }

    @Override
    public @NonNull
    Mono<Boolean> existsById(@NonNull ID id) {
        Assert.notNull(id, "Id must not be null in existsById");
        RelationalPersistentEntity<?> entity =
                lcClient.getMappingContext().getRequiredPersistentEntity(entityInfo.getJavaType());
        RelationalPersistentProperty idProperty = entity.getRequiredIdProperty();
        Table table = Table.create(entity.getTableName());
        Column idColumn = Column.create(idProperty.getColumnName(), table);
        Object idValue = lcClient.getSchemaDialect().convertToDataBase(id, idProperty);
        SqlQuery<Select> q = new SqlQuery<>(lcClient);
        Select select =
                Select.builder()
                        .select(idColumn)
                        .from(table)
                        .limit(1)
                        .where(Conditions.isEqual(idColumn, q.marker(idValue)))
                        .build();
        q.setQuery(select);
        return q.execute().map((r, m) -> r).first().hasElement();
    }

    @Override
    public @NonNull
    <S extends T> Mono<S> save(@NonNull S objectToSave) {
        return lcClient.save(objectToSave);
    }

    @Override
    public @NonNull
    <S extends T> Flux<S> saveAll(@NonNull Iterable<S> objectsToSave) {
        return lcClient.save(objectsToSave);
    }

    @Override
    public @NonNull
    <S extends T> Flux<S> saveAll(@NonNull Publisher<S> objectsToSave) {
        return lcClient.save(objectsToSave);
    }

    @Override
    public @NonNull
    Mono<Void> delete(@NonNull T objectToDelete) {
        return lcClient.delete(objectToDelete);
    }

    @Override
    public @NonNull
    Mono<Void> deleteAll(@NonNull Iterable<? extends T> iterable) {
        return lcClient.delete(iterable);
    }

    @Override
    public @NonNull Mono<Void> deleteAll(@NonNull Publisher<? extends T> objectPublisher) {
        return lcClient.delete(objectPublisher);
    }

    @Override
    public @NonNull Mono<Void> deleteAll() {
        if (ModelUtils.hasCascadeDeleteImpacts(
                entityInfo.getJavaType(), lcClient.getMappingContext())) {
            return deleteAll(findAll());
        }
        return entityOperations.delete(entityInfo.getJavaType()).all().then();
    }

    @Override
    public @NonNull Mono<Void> deleteAllById(@NonNull Iterable<? extends ID> ids) {
        RelationalPersistentEntity<?> entity =
                lcClient.getMappingContext().getRequiredPersistentEntity(entityInfo.getJavaType());
        RelationalPersistentProperty idProperty = entity.getRequiredIdProperty();

        Table table = Table.create(entity.getTableName());
        Column idColumn = Column.create(idProperty.getColumnName(), table);

        SqlQuery<Delete> q = new SqlQuery<>(lcClient);
        List<Expression> idsList =
                Streamable.of(ids)
                        .map(
                                id ->
                                        q.marker(
                                                lcClient.getSchemaDialect()
                                                        .convertToDataBase(id, idProperty)))
                        .toList();

        q.setQuery(Delete.builder().from(table).where(Conditions.in(idColumn, idsList)).build());
        return q.execute().fetch().rowsUpdated().then();
    }

    @Override
    public @NonNull Mono<Void> deleteById(@NonNull ID id) {
        Assert.notNull(id, "Id must not be null in deleteById");
        if (ModelUtils.hasCascadeDeleteImpacts(
                entityInfo.getJavaType(), lcClient.getMappingContext())) {
            return findById(id).flatMap(this::delete);
        }
        RelationalPersistentEntity<?> entity =
                lcClient.getMappingContext().getRequiredPersistentEntity(entityInfo.getJavaType());
        if (!entity.hasIdProperty()) {
            return findById(id).flatMap(this::delete);
        }
        RelationalPersistentProperty idProperty = entity.getRequiredIdProperty();
        Table table = Table.create(entity.getTableName());
        Column idColumn = Column.create(idProperty.getColumnName(), table);
        Object idValue = lcClient.getSchemaDialect().convertToDataBase(id, idProperty);

        SqlQuery<Delete> q = new SqlQuery<>(lcClient);
        q.setQuery(
                Delete.builder()
                        .from(table)
                        .where(Conditions.isEqual(idColumn, q.marker(idValue)))
                        .build());
        return q.execute().fetch().rowsUpdated().then();
    }

    @Override
    public @NonNull Mono<Void> deleteById(@NonNull Publisher<ID> idPublisher) {
        if (ModelUtils.hasCascadeDeleteImpacts(
                entityInfo.getJavaType(), lcClient.getMappingContext())) {
            return deleteAll(findAllById(idPublisher));
        }
        RelationalPersistentEntity<?> entity =
                lcClient.getMappingContext().getRequiredPersistentEntity(entityInfo.getJavaType());
        if (!entity.hasIdProperty()) {
            return deleteAll(findAllById(idPublisher));
        }
        return Flux.from(idPublisher)
                .subscribeOn(Schedulers.parallel())
                .publishOn(Schedulers.parallel())
                .buffer(100)
                .parallel()
                .runOn(Schedulers.parallel(), 1)
                .flatMap(this::deleteAllById)
                .then();
    }
}
