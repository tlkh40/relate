package me.lusory.relate.query.operation;

import me.lusory.relate.mapping.LcEntityWriter;
import me.lusory.relate.model.ModelUtils;
import me.lusory.relate.query.InsertMultiple;
import me.lusory.relate.annotations.ForeignKey;
import me.lusory.relate.annotations.GeneratedValue;
import me.lusory.relate.model.EntityState;
import me.lusory.relate.model.LcEntityTypeInfo.ForeignTableInfo;
import me.lusory.relate.model.ModelAccessException;
import me.lusory.relate.query.SqlQuery;
import org.apache.commons.lang3.mutable.MutableObject;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.*;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.r2dbc.core.RowsFetchSpec;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.*;

class SaveProcessor extends AbstractInstanceProcessor<SaveProcessor.SaveRequest> {

    private static void removeForeignTableLink(
            Operation op,
            SaveRequest request,
            ForeignTableInfo foreignTableInfo,
            Object originalValue) {
        try {
            if (foreignTableInfo.isCollection()) {
                ModelUtils.removeFromCollectionField(
                        foreignTableInfo.getField(), originalValue, request.instance);
            } else {
                EntityState foreignState = EntityState.get(originalValue, op.lcClient);
                foreignState.setForeignTableField(
                        originalValue, foreignTableInfo.getField(), null, false);
            }
        } catch (Exception e) {
            throw new ModelAccessException("Unable to remove link for removed entity", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> void processForeignTableFieldCollection(
            Operation op,
            SaveRequest request,
            Field foreignTableField,
            MutableObject<?> foreignFieldValue,
            RelationalPersistentEntity<T> foreignEntity,
            RelationalPersistentProperty fkProperty,
            ForeignKey fkAnnotation) {
        Object value = foreignFieldValue.getValue();
        Object originalValue = request.state.getPersistedValue(foreignTableField.getName());
        if (value == null) {
            if (originalValue == null) {
                return; // was already empty
            }
            value = new ArrayList<>(0);
        }
        List<Object> deletedElements = new LinkedList<>();
        if (originalValue != null) {
            deletedElements.addAll(ModelUtils.getAsCollection(originalValue));
        }
        deletedElements.removeAll(ModelUtils.getAsCollection(value));

        if (!deletedElements.isEmpty()) {
            deletedElements(op, deletedElements, foreignEntity, fkProperty, fkAnnotation);
        }

        for (Object element : ModelUtils.getAsCollection(value)) {
            SaveRequest save = op.addToSave((T) element, foreignEntity, null, null);
            save.state.setPersistedField(element, fkProperty.getField(), request.instance, false);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> void deletedElements(
            Operation op,
            List<Object> deletedElements,
            RelationalPersistentEntity<T> foreignEntity,
            RelationalPersistentProperty fkProperty,
            ForeignKey fkAnnotation) {
        if (!fkAnnotation.optional()
                || fkAnnotation.onForeignDeleted().equals(ForeignKey.OnForeignDeleted.DELETE)) {
            // delete
            for (Object element : deletedElements) {
                op.addToDelete((T) element, foreignEntity, null, null);
            }
        } else {
            // update to null
            for (Object element : deletedElements) {
                SaveRequest save = op.addToSave((T) element, foreignEntity, null, null);
                save.state.setPersistedField(element, fkProperty.getField(), null, false);
            }
        }
    }

    private static <T> void processForeignTableFieldSimple(
            Operation op,
            SaveRequest request,
            Field foreignTableField,
            MutableObject<?> foreignFieldValue,
            RelationalPersistentEntity<T> foreignEntity,
            RelationalPersistentProperty fkProperty,
            ForeignKey fkAnnotation) {
        Object value = foreignFieldValue.getValue();
        @SuppressWarnings("unchecked")
        T originalValue = (T) request.state.getPersistedValue(foreignTableField.getName());
        if (!Objects.equals(originalValue, value) && originalValue != null) {
            // it has been changed, we need to update/delete the previous one
            if (!fkAnnotation.optional()
                    || fkAnnotation.onForeignDeleted().equals(ForeignKey.OnForeignDeleted.DELETE)) {
                // delete
                op.addToDelete(originalValue, foreignEntity, null, null);
            } else {
                // update to null
                SaveRequest save = op.addToSave(originalValue, foreignEntity, null, null);
                save.state.setPersistedField(originalValue, fkProperty.getField(), null, false);
            }
        }
        if (value != null) {
            // save value
            @SuppressWarnings("unchecked")
            SaveRequest save = op.addToSave((T) value, foreignEntity, null, null);
            save.state.setPersistedField(value, fkProperty.getField(), request.instance, false);
        }
    }

    private static void doInsert(
            Operation op,
            RelationalPersistentEntity<?> entityType,
            List<SaveRequest> requests,
            List<Mono<Void>> statements) {
        if (requests.isEmpty()) {
            return;
        }
        List<SaveRequest> remaining = requests;
        do {
            if (remaining.size() == 1) {
                statements.add(doInsertSingle(op, remaining.get(0)));
                return;
            }
            if (remaining.size() <= 1000) {
                statements.add(doInsertMultiple(op, entityType, remaining));
                return;
            }
            statements.add(doInsertMultiple(op, entityType, remaining.subList(0, 1000)));
            remaining = remaining.subList(1000, remaining.size());
        } while (true);
    }

    @SuppressWarnings({"java:S1612", "java:S3776"}) // cannot do it
    private static Mono<Void> doInsertMultiple(
            Operation op, RelationalPersistentEntity<?> entityType, List<SaveRequest> requests) {
        return Flux.defer(
                        () -> {
                            SqlQuery<InsertMultiple> query = new SqlQuery<>(op.lcClient);
                            // table
                            Table table = Table.create(entityType.getTableName());
                            // columns
                            final List<Column> columns = new LinkedList<>();
                            final List<RelationalPersistentProperty> generated = new LinkedList<>();
                            for (RelationalPersistentProperty property : entityType) {
                                if (property.isAnnotationPresent(GeneratedValue.class)) {
                                    GeneratedValue gv =
                                            property.getRequiredAnnotation(GeneratedValue.class);
                                    if (GeneratedValue.Strategy.SEQUENCE.equals(gv.strategy())) {
                                        columns.add(Column.create(property.getColumnName(), table));
                                        generated.add(property);
                                    } else if (GeneratedValue.Strategy.RANDOM_UUID.equals(
                                            gv.strategy())
                                            && !op.lcClient
                                            .getSchemaDialect()
                                            .supportsUuidGeneration()) {
                                        columns.add(Column.create(property.getColumnName(), table));
                                    } else {
                                        generated.add(property);
                                    }
                                } else if (!property.isTransient()) {
                                    columns.add(Column.create(property.getColumnName(), table));
                                }
                            }

                            // values
                            List<List<Expression>> rows = new LinkedList<>();
                            for (SaveRequest request : requests) {
                                Map<SqlIdentifier, Expression> generatedValues = new HashMap<>();
                                OutboundRow row = new OutboundRow();
                                LcEntityWriter writer = new LcEntityWriter(op.lcClient.getMapper());
                                long currentDate = System.currentTimeMillis();
                                for (RelationalPersistentProperty property : request.entityType) {
                                    if (property.isAnnotationPresent(GeneratedValue.class)) {
                                        GeneratedValue gv =
                                                property.getRequiredAnnotation(
                                                        GeneratedValue.class);
                                        if (gv.strategy()
                                                .equals(GeneratedValue.Strategy.SEQUENCE)) {
                                            generatedValues.put(
                                                    property.getColumnName(),
                                                    SimpleFunction.create(
                                                            op.lcClient
                                                                    .getSchemaDialect()
                                                                    .sequenceNextValueFunctionName(),
                                                            Collections.singletonList(
                                                                    SQL.literalOf(gv.sequence()))));
                                        } else if (GeneratedValue.Strategy.RANDOM_UUID.equals(
                                                gv.strategy())
                                                && !op.lcClient
                                                .getSchemaDialect()
                                                .supportsUuidGeneration()) {
                                            UUID uuid = UUID.randomUUID();
                                            request.accessor.setProperty(property, uuid);
                                            writer.writeProperty(row, property, request.accessor);
                                        }
                                    } else if (!property.isTransient()) {
                                        if (request.entityType.isVersionProperty(property)) {
                                            // Version 1 for an insert
                                            request.accessor.setProperty(
                                                    property,
                                                    op.lcClient
                                                            .getMapper()
                                                            .getConversionService()
                                                            .convert(1L, property.getType()));
                                        } else if (property.isAnnotationPresent(CreatedDate.class)
                                                || property.isAnnotationPresent(
                                                LastModifiedDate.class)) {
                                            request.accessor.setProperty(
                                                    property,
                                                    getDateValue(currentDate, property.getType()));
                                        }
                                        writer.writeProperty(row, property, request.accessor);
                                    }
                                }
                                List<Expression> values = new ArrayList<>(columns.size());
                                for (Column col : columns) {
                                    Expression value = generatedValues.get(col.getReferenceName());
                                    if (value != null) {
                                        values.add(value);
                                    } else {
                                        Parameter val = row.get(col.getReferenceName());
                                        if (val.getValue() == null) {
                                            values.add(SQL.nullLiteral());
                                        } else {
                                            values.add(query.marker(val.getValue()));
                                        }
                                    }
                                }
                                rows.add(values);
                            }

                            query.setQuery(new InsertMultiple(table, columns, rows));
                            LinkedList<SaveRequest> queue = new LinkedList<>(requests);

                            return query.execute()
                                    .filter(statement -> statement.returnGeneratedValues())
                                    .map(
                                            (r, meta) -> {
                                                SaveRequest request = queue.removeFirst();
                                                int index = 0;
                                                for (RelationalPersistentProperty property :
                                                        generated) {
                                                    request.accessor.setProperty(
                                                            property,
                                                            op.lcClient
                                                                    .getSchemaDialect()
                                                                    .convertFromDataBase(
                                                                            r.get(index++),
                                                                            property.getType()));
                                                }
                                                request.state.loaded(request.instance);
                                                return request.instance;
                                            })
                                    .all();
                        })
                .then();
    }

    @SuppressWarnings({"java:S1612", "java:S3776"}) // cannot do it
    private static Mono<Void> doInsertSingle(Operation op, SaveRequest request) {
        return Mono.fromCallable(
                        () -> {
                            SqlQuery<Insert> query = new SqlQuery<>(op.lcClient);
                            final List<RelationalPersistentProperty> generated = new LinkedList<>();
                            OutboundRow row = new OutboundRow();
                            LcEntityWriter writer = new LcEntityWriter(op.lcClient.getMapper());
                            long currentDate = System.currentTimeMillis();
                            for (RelationalPersistentProperty property : request.entityType) {
                                if (property.isAnnotationPresent(GeneratedValue.class)) {
                                    GeneratedValue gv =
                                            property.getRequiredAnnotation(GeneratedValue.class);
                                    if (GeneratedValue.Strategy.RANDOM_UUID.equals(gv.strategy())
                                            && !op.lcClient
                                            .getSchemaDialect()
                                            .supportsUuidGeneration()) {
                                        UUID uuid = UUID.randomUUID();
                                        request.accessor.setProperty(property, uuid);
                                        writer.writeProperty(row, property, request.accessor);
                                    } else {
                                        generated.add(property);
                                    }
                                } else if (!property.isTransient()) {
                                    if (request.entityType.isVersionProperty(property)) {
                                        // Version 1 for an insert
                                        request.accessor.setProperty(
                                                property,
                                                op.lcClient
                                                        .getMapper()
                                                        .getConversionService()
                                                        .convert(1L, property.getType()));
                                    } else if (property.isAnnotationPresent(CreatedDate.class)
                                            || property.isAnnotationPresent(
                                            LastModifiedDate.class)) {
                                        request.accessor.setProperty(
                                                property,
                                                getDateValue(currentDate, property.getType()));
                                    }
                                    writer.writeProperty(row, property, request.accessor);
                                }
                            }

                            query.setQuery(
                                    createInsertQuery(
                                            query,
                                            row,
                                            request.entityType.getTableName(),
                                            generated));

                            return query.execute()
                                    .filter(statement -> statement.returnGeneratedValues())
                                    .map(
                                            (r, meta) -> {
                                                int index = 0;
                                                for (RelationalPersistentProperty property :
                                                        generated) {
                                                    request.accessor.setProperty(
                                                            property,
                                                            op.lcClient
                                                                    .getSchemaDialect()
                                                                    .convertFromDataBase(
                                                                            r.get(index++),
                                                                            property.getType()));
                                                }
                                                request.state.loaded(request.instance);
                                                return request.instance;
                                            });
                        })
                .flatMap(RowsFetchSpec::first)
                .then();
    }

    private static Insert createInsertQuery(
            SqlQuery<Insert> query,
            OutboundRow row,
            SqlIdentifier tableName,
            List<RelationalPersistentProperty> generated) {
        Table table = Table.create(tableName);
        List<Column> columns = new ArrayList<>(row.size());
        List<Expression> values = new ArrayList<>(row.size());
        for (RelationalPersistentProperty property : generated) {
            GeneratedValue gv = property.getRequiredAnnotation(GeneratedValue.class);
            if (gv.strategy().equals(GeneratedValue.Strategy.SEQUENCE)) {
                columns.add(Column.create(property.getColumnName(), table));
                values.add(
                        SimpleFunction.create(
                                query.getClient()
                                        .getSchemaDialect()
                                        .sequenceNextValueFunctionName(),
                                Collections.singletonList(SQL.literalOf(gv.sequence()))));
            }
        }
        for (Map.Entry<SqlIdentifier, Parameter> entry : row.entrySet()) {
            columns.add(Column.create(entry.getKey(), table));
            if (entry.getValue().getValue() == null) {
                values.add(SQL.nullLiteral());
            } else {
                values.add(query.marker(entry.getValue().getValue()));
            }
        }
        return Insert.builder().into(table).columns(columns).values(values).build();
    }

    private static Mono<Void> doUpdate(Operation op, SaveRequest request) {
        return Mono.fromCallable(() -> createUpdateRequest(op, request))
                .flatMap(
                        updatedRows ->
                                updatedRows != null
                                        ? updatedRows
                                        .doOnSuccess(nb -> entityUpdated(op, request))
                                        .then()
                                        : Mono.empty());
    }

    private static Mono<Integer> createUpdateRequest(Operation op, SaveRequest request) {
        SqlQuery<Update> query = new SqlQuery<>(op.lcClient);
        Table table = Table.create(request.entityType.getTableName());
        OutboundRow row = new OutboundRow();
        LcEntityWriter writer = new LcEntityWriter(op.lcClient.getMapper());
        List<AssignValue> assignments = new LinkedList<>();
        if (!prepareUpdate(request, table, assignments, row, writer, query)) {
            return null;
        }

        for (Map.Entry<SqlIdentifier, Parameter> entry : row.entrySet()) {
            assignments.add(
                    AssignValue.create(
                            Column.create(entry.getKey(), table),
                            entry.getValue().getValue() != null
                                    ? query.marker(entry.getValue().getValue())
                                    : SQL.nullLiteral()));
        }

        Condition criteria =
                ModelUtils.getConditionOnId(
                        query, request.entityType, request.accessor, op.lcClient);

        if (request.entityType.hasVersionProperty()) {
            RelationalPersistentProperty property = request.entityType.getRequiredVersionProperty();
            Object value = request.accessor.getProperty(property);
            Assert.notNull(value, "Version must not be null");
            long currentVersion = ((Number) value).longValue();
            criteria =
                    criteria.and(
                            Conditions.isEqual(
                                    Column.create(property.getColumnName(), table),
                                    query.marker(currentVersion)));
        }

        query.setQuery(Update.builder().table(table).set(assignments).where(criteria).build());
        Mono<Integer> rowsUpdated = query.execute().fetch().rowsUpdated();
        if (request.entityType.hasVersionProperty()) {
            rowsUpdated =
                    rowsUpdated.flatMap(
                            updatedRows -> {
                                if (updatedRows == 0) {
                                    return Mono.error(
                                            new OptimisticLockingFailureException(
                                                    "Version does not match"));
                                }
                                return Mono.just(updatedRows);
                            });
        }
        return rowsUpdated;
    }

    private static boolean prepareUpdate(
            SaveRequest request,
            Table table,
            List<AssignValue> assignments,
            OutboundRow row,
            LcEntityWriter writer,
            SqlQuery<Update> query) {
        boolean hasUpdate = false;
        Map<RelationalPersistentProperty, Object> propertiesToSetIfUpdate = new HashMap<>();
        long currentDate = System.currentTimeMillis();
        for (RelationalPersistentProperty property : request.entityType) {
            if (request.entityType.isVersionProperty(property)) {
                Object value = request.accessor.getProperty(property);
                Assert.notNull(
                        value,
                        "Version must not be null (property "
                                + property.getName()
                                + " on "
                                + request.entityType.getType().getSimpleName()
                                + ")");
                long currentVersion = ((Number) value).longValue();
                assignments.add(
                        AssignValue.create(
                                Column.create(property.getColumnName(), table),
                                query.marker(currentVersion + 1)));
            } else if (property.isAnnotationPresent(LastModifiedDate.class)) {
                propertiesToSetIfUpdate.put(
                        property, getDateValue(currentDate, property.getType()));
            } else if (request.state.isFieldModified(property.getName())) {
                if (ModelUtils.isUpdatable(property)) {
                    writer.writeProperty(row, property, request.accessor);
                    hasUpdate = true;
                } else {
                    request.state.restorePersistedValue(request.instance, property.getField());
                }
            }
        }
        if (hasUpdate) {
            for (Map.Entry<RelationalPersistentProperty, Object> e :
                    propertiesToSetIfUpdate.entrySet()) {
                request.accessor.setProperty(e.getKey(), e.getValue());
                writer.writeProperty(row, e.getKey(), request.accessor);
            }
        }
        return hasUpdate;
    }

    private static void entityUpdated(Operation op, SaveRequest request) {
        request.state.load(request.instance).subscribe();
        if (request.entityType.hasVersionProperty()) {
            RelationalPersistentProperty property = request.entityType.getRequiredVersionProperty();
            Object version = request.accessor.getProperty(property);
            Assert.notNull(version, "Version must not be null");
            request.accessor.setProperty(
                    property,
                    op.lcClient
                            .getMapper()
                            .getConversionService()
                            .convert(((Number) version).longValue() + 1, property.getType()));
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T getDateValue(long timestamp, Class<T> type) {
        if (type.equals(long.class) || type.equals(Long.class)) {
            return (T) Long.valueOf(timestamp);
        }
        if (type.isAssignableFrom(java.time.Instant.class)) {
            return (T) java.time.Instant.ofEpochMilli(timestamp);
        }
        if (type.isAssignableFrom(java.time.LocalDate.class)) {
            return (T) java.time.Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDate();
        }
        if (type.isAssignableFrom(java.time.LocalTime.class)) {
            return (T) java.time.Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalTime();
        }
        if (type.isAssignableFrom(java.time.OffsetTime.class)) {
            return (T)
                    java.time.OffsetTime.ofInstant(
                            java.time.Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        }
        if (type.isAssignableFrom(java.time.LocalDateTime.class)) {
            return (T)
                    java.time.LocalDateTime.ofInstant(
                            java.time.Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        }
        if (type.isAssignableFrom(java.time.ZonedDateTime.class)) {
            return (T)
                    java.time.ZonedDateTime.ofInstant(
                            java.time.Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        }
        return null;
    }

    @Override
    protected <T> SaveRequest createRequest(
            T instance,
            EntityState state,
            RelationalPersistentEntity<T> entity,
            PersistentPropertyAccessor<T> accessor) {
        return new SaveRequest(entity, instance, state, accessor);
    }

    @Override
    protected boolean doProcess(Operation op, SaveRequest request) {
        return true;
    }

    @Override
    protected void processForeignKey(
            Operation op,
            SaveRequest request,
            RelationalPersistentProperty fkProperty,
            ForeignKey fkAnnotation,
            @Nullable ForeignTableInfo foreignTableInfo) {
        Object value = request.accessor.getProperty(fkProperty);
        Object originalValue = request.state.getPersistedValue(fkProperty.getName());
        if (!Objects.equals(originalValue, value) && originalValue != null) {
            // link changed, we need to delete/null the previous one
            // remove the link
            if (foreignTableInfo != null) {
                removeForeignTableLink(op, request, foreignTableInfo, originalValue);
            }
            if ((foreignTableInfo != null && !foreignTableInfo.getAnnotation().optional())
                    || fkAnnotation.cascadeDelete()) {
                // not optional specified on ForeignTable, or cascadeDelete -> this is a delete
                op.addToDelete(originalValue, null, null, null);
            }
        }
        if (value != null) {
            SaveRequest save = op.addToSave(value, null, null, null);
            if (!save.state.isPersisted()) {
                request.dependsOn(save); // if the foreign id is not yet available, we depend on it
            }
        }
    }

    @Override
    protected <T> void processForeignTableField(
            Operation op,
            SaveRequest request,
            ForeignTableInfo foreignTableInfo,
            @Nullable MutableObject<?> foreignFieldValue,
            RelationalPersistentEntity<T> foreignEntity,
            RelationalPersistentProperty fkProperty,
            ForeignKey fkAnnotation) {
        if (foreignFieldValue == null) {
            return; // not loaded -> not saved
        }
        if (foreignTableInfo.isCollection()) {
            processForeignTableFieldCollection(
                    op,
                    request,
                    foreignTableInfo.getField(),
                    foreignFieldValue,
                    foreignEntity,
                    fkProperty,
                    fkAnnotation);
        } else {
            processForeignTableFieldSimple(
                    op,
                    request,
                    foreignTableInfo.getField(),
                    foreignFieldValue,
                    foreignEntity,
                    fkProperty,
                    fkAnnotation);
        }
    }

    @Override
    protected Mono<Void> doRequests(
            Operation op, RelationalPersistentEntity<?> entityType, List<SaveRequest> requests) {
        List<Mono<Void>> statements = new LinkedList<>();
        boolean multipleInsertSupported =
                op.lcClient.getSchemaDialect().isMultipleInsertSupported();
        List<SaveRequest> toInsert = new LinkedList<>();
        for (SaveRequest request : requests) {
            if (!request.state.isPersisted()) {
                if (!multipleInsertSupported) {
                    statements.add(doInsertSingle(op, request));
                } else {
                    toInsert.add(request);
                }
            } else {
                statements.add(doUpdate(op, request));
            }
        }
        doInsert(op, entityType, toInsert, statements);
        return Operation.executeParallel(statements);
    }

    static class SaveRequest extends AbstractInstanceProcessor.Request {

        <T> SaveRequest(
                RelationalPersistentEntity<T> entityType,
                T instance,
                EntityState state,
                PersistentPropertyAccessor<T> accessor) {
            super(entityType, instance, state, accessor);
            if (!this.state.isLoaded() && this.state.isPersisted()) {
                this.toProcess = false;
            }
        }
    }
}
