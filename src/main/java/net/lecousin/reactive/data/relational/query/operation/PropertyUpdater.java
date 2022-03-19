package net.lecousin.reactive.data.relational.query.operation;

import net.lecousin.reactive.data.relational.model.ModelUtils;
import net.lecousin.reactive.data.relational.query.SqlQuery;
import net.lecousin.reactive.data.relational.query.operation.SaveProcessor.SaveRequest;
import net.lecousin.reactive.data.relational.sql.ColumnIncrement;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.*;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Mono;

import java.util.*;

class PropertyUpdater extends AbstractProcessor<PropertyUpdater.Request> {

    private final Map<
            RelationalPersistentEntity<?>,
            Map<RelationalPersistentProperty, Map<Object, Request>>>
            requests = new HashMap<>();

    private static void executeUpdates(
            Operation op,
            Map<Object, Set<Object>> reverseMap,
            RelationalPersistentEntity<?> entityType,
            RelationalPersistentProperty property,
            @Nullable RelationalPersistentProperty versionProperty,
            List<Request> ready,
            List<Mono<Void>> calls) {
        Table table = Table.create(entityType.getTableName());
        for (Map.Entry<Object, Set<Object>> update : reverseMap.entrySet()) {
            SqlQuery<Update> query = new SqlQuery<>(op.lcClient);
            List<Expression> values = new ArrayList<>(update.getValue().size());
            for (Object value : update.getValue()) {
                values.add(query.marker(value));
            }
            List<AssignValue> assignments = new LinkedList<>();
            assignments.add(
                    AssignValue.create(
                            Column.create(property.getColumnName(), table),
                            update.getKey() != null
                                    ? query.marker(update.getKey())
                                    : SQL.nullLiteral()));
            if (versionProperty != null) {
                assignments.add(
                        AssignValue.create(
                                Column.create(versionProperty.getColumnName(), table),
                                SQL.literalOf(
                                        new ColumnIncrement(
                                                Column.create(
                                                        versionProperty.getColumnName(), table),
                                                op.lcClient))));
            }
            Condition where = Conditions.in(Column.create(property.getColumnName(), table), values);
            for (SaveRequest save :
                    op.save.getPendingRequests(
                            entityType,
                            s ->
                                    update.getValue()
                                            .contains(
                                                    ModelUtils.getPersistedDatabaseValue(
                                                            s.state,
                                                            property,
                                                            op.lcClient.getMappingContext())))) {
                if (save.state.isPersisted()) {
                    where =
                            where.and(
                                    ModelUtils.getConditionOnId(
                                                    query,
                                                    save.entityType,
                                                    save.accessor,
                                                    op.lcClient)
                                            .not());
                }
                save.accessor.setProperty(property, update.getKey());
            }
            query.setQuery(Update.builder().table(table).set(assignments).where(where).build());
            calls.add(
                    query.execute().then().doOnSuccess(v -> ready.forEach(r -> r.executed = true)));
        }
    }

    Request update(
            RelationalPersistentEntity<?> entityType,
            RelationalPersistentProperty property,
            Object whereValueIs,
            Object newValue) {
        Map<RelationalPersistentProperty, Map<Object, Request>> map =
                requests.computeIfAbsent(entityType, e -> new HashMap<>());
        Map<Object, Request> map2 = map.computeIfAbsent(property, p -> new HashMap<>());
        return map2.computeIfAbsent(
                whereValueIs, e -> new Request(entityType, property, whereValueIs, newValue));
    }

    @Override
    @SuppressWarnings("java:S3776")
    protected Mono<Void> executeRequests(Operation op) {
        List<Mono<Void>> calls = new LinkedList<>();
        for (Map.Entry<
                RelationalPersistentEntity<?>,
                Map<RelationalPersistentProperty, Map<Object, Request>>>
                entity : requests.entrySet()) {
            RelationalPersistentProperty versionProperty = entity.getKey().getVersionProperty();
            for (Map.Entry<RelationalPersistentProperty, Map<Object, Request>> property :
                    entity.getValue().entrySet()) {
                Map<Object, Set<Object>> reverseMap = new HashMap<>();
                List<Request> ready = new LinkedList<>();
                for (Map.Entry<Object, Request> entry : property.getValue().entrySet()) {
                    if (!canExecuteRequest(entry.getValue())) {
                        continue;
                    }
                    Set<Object> set =
                            reverseMap.computeIfAbsent(
                                    entry.getValue().newValue, e -> new HashSet<>());
                    set.add(entry.getKey());
                    ready.add(entry.getValue());
                }
                if (reverseMap.isEmpty()) {
                    continue;
                }
                executeUpdates(
                        op,
                        reverseMap,
                        entity.getKey(),
                        property.getKey(),
                        versionProperty,
                        ready,
                        calls);
            }
        }
        return Operation.executeParallel(calls);
    }

    static class Request extends AbstractProcessor.Request {
        RelationalPersistentProperty property;
        Object whereValueIs;
        Object newValue;

        Request(
                RelationalPersistentEntity<?> entityType,
                RelationalPersistentProperty property,
                Object whereValueIs,
                Object newValue) {
            super(entityType);
            this.property = property;
            this.whereValueIs = whereValueIs;
            this.newValue = newValue;
        }
    }
}
