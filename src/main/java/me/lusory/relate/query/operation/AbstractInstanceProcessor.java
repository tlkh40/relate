package me.lusory.relate.query.operation;

import me.lusory.relate.annotations.ForeignKey;
import me.lusory.relate.model.EntityState;
import me.lusory.relate.model.LcEntityTypeInfo;
import me.lusory.relate.model.LcEntityTypeInfo.ForeignTableInfo;
import me.lusory.relate.model.ModelAccessException;
import org.apache.commons.lang3.mutable.MutableObject;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.Predicate;

abstract class AbstractInstanceProcessor<R extends AbstractInstanceProcessor.Request>
        extends AbstractProcessor<R> {

    /**
     * Requests, by table, by instance.
     */
    private final Map<RelationalPersistentEntity<?>, Map<Object, R>> requests = new HashMap<>();

    public <T> R addToProcess(
            Operation op,
            T instance,
            @Nullable RelationalPersistentEntity<T> entity,
            @Nullable EntityState state,
            @Nullable PersistentPropertyAccessor<T> accessor) {
        return addRequest(op, instance, entity, state, accessor);
    }

    public <T> R addToNotProcess(
            Operation op,
            T instance,
            @Nullable RelationalPersistentEntity<T> entity,
            @Nullable EntityState state,
            @Nullable PersistentPropertyAccessor<T> accessor) {
        R request = addRequest(op, instance, entity, state, accessor);
        request.toProcess = false;
        return request;
    }

    List<R> getPendingRequests(RelationalPersistentEntity<?> entity, Predicate<R> predicate) {
        List<R> list = new LinkedList<>();
        Map<Object, R> map = requests.get(entity);
        if (map == null) {
            return list;
        }
        for (R request : map.values()) {
            if (request.toProcess
                    && !request.executed
                    && request.state.isPersisted()
                    && request.state.isLoaded()
                    && predicate.test(request)) {
                list.add(request);
            }
        }
        return list;
    }

    boolean processRequests(Operation op) {
        boolean somethingProcessed = false;
        for (Map<Object, R> map : new ArrayList<>(requests.values())) {
            for (R request : new ArrayList<>(map.values())) {
                somethingProcessed |= process(op, request);
            }
        }
        return somethingProcessed;
    }

    private boolean process(Operation op, R request) {
        if (request.processed || !request.toProcess) {
            return false;
        }
        request.processed = true;

        if (!doProcess(op, request)) {
            return false;
        }

        processForeignKeys(op, request);
        processForeignTables(op, request);

        return true;
    }

    private void processForeignKeys(Operation op, R request) {
        for (RelationalPersistentProperty property : request.entityType) {
            ForeignKey fkAnnotation = property.findAnnotation(ForeignKey.class);
            if (fkAnnotation != null) {
                ForeignTableInfo fti =
                        LcEntityTypeInfo.get(property.getActualType())
                                .getForeignTableWithFieldForJoinKey(
                                        property.getName(), request.entityType.getType());
                processForeignKey(op, request, property, fkAnnotation, fti);
            }
        }
    }

    private void processForeignTables(Operation op, R request) {
        for (ForeignTableInfo fti :
                LcEntityTypeInfo.get(request.instance.getClass()).getForeignTables()) {
            boolean isCollection = fti.isCollection();
            RelationalPersistentEntity<?> foreignEntity =
                    op.lcClient
                            .getMappingContext()
                            .getRequiredPersistentEntity(
                                    isCollection
                                            ? fti.getCollectionElementType()
                                            : fti.getField().getType());
            RelationalPersistentProperty fkProperty =
                    foreignEntity.getRequiredPersistentProperty(fti.getAnnotation().joinKey());
            ForeignKey fk = fkProperty.findAnnotation(ForeignKey.class);
            MutableObject<?> foreignFieldValue;
            try {
                foreignFieldValue =
                        request.state.getForeignTableField(
                                request.instance, fti.getField().getName());
            } catch (Exception e) {
                throw new ModelAccessException("Unable to get foreign table field", e);
            }

            processForeignTableField(op, request, fti, foreignFieldValue, foreignEntity, fkProperty, fk);
        }
    }

    protected abstract <T> R createRequest(
            T instance,
            EntityState state,
            RelationalPersistentEntity<T> entity,
            PersistentPropertyAccessor<T> accessor);

    protected abstract boolean doProcess(Operation op, R request);

    protected abstract void processForeignKey(
            Operation op,
            R request,
            RelationalPersistentProperty fkProperty,
            ForeignKey fkAnnotation,
            @Nullable ForeignTableInfo foreignTableInfo
    );

    @SuppressWarnings("java:S107")
    protected abstract <T> void processForeignTableField(
            Operation op,
            R request,
            ForeignTableInfo foreignTableInfo,
            @Nullable MutableObject<?> foreignFieldValue,
            RelationalPersistentEntity<T> foreignEntity,
            RelationalPersistentProperty fkProperty,
            ForeignKey fkAnnotation);

    @SuppressWarnings({"java:S3824", "unchecked"})
    private <T> R addRequest(
            Operation op,
            T instance,
            @Nullable RelationalPersistentEntity<T> entity,
            @Nullable EntityState state,
            @Nullable PersistentPropertyAccessor<T> accessor) {
        if (entity == null) {
            entity =
                    (RelationalPersistentEntity<T>)
                            op.lcClient
                                    .getMappingContext()
                                    .getRequiredPersistentEntity(instance.getClass());
        }
        if (accessor == null) {
            accessor = entity.getPropertyAccessor(instance);
        }
        if (state == null) {
            state = EntityState.get(instance, op.lcClient, entity);
        }
        instance = op.cache.getOrSet(state, entity, accessor, op.lcClient);
        Map<Object, R> map = requests.computeIfAbsent(entity, e -> new HashMap<>());
        R r = map.get(instance);
        if (r == null) {
            r = createRequest(instance, state, entity, accessor);
            map.put(instance, r);
        }
        return r;
    }

    @Override
    protected Mono<Void> executeRequests(Operation op) {
        List<Mono<Void>> executions = new LinkedList<>();
        for (Map.Entry<RelationalPersistentEntity<?>, Map<Object, R>> entity :
                requests.entrySet()) {
            List<R> ready = new LinkedList<>();
            for (R request : entity.getValue().values()) {
                if (canExecuteRequest(request)) {
                    ready.add(request);
                }
            }
            if (!ready.isEmpty()) {
                Mono<Void> execution = doRequests(op, entity.getKey(), ready);
                if (execution != null) {
                    executions.add(
                            execution.doOnSuccess(
                                    v -> {
                                        for (R r : ready) {
                                            r.executed = true;
                                        }
                                    }));
                } else {
                    ready.forEach(r -> r.executed = true);
                }
            }
        }
        return Operation.executeParallel(executions);
    }

    protected abstract Mono<Void> doRequests(
            Operation op, RelationalPersistentEntity<?> entityType, List<R> requests);

    abstract static class Request extends AbstractProcessor.Request {
        Object instance;
        EntityState state;
        PersistentPropertyAccessor<?> accessor;

        boolean processed = false;
        boolean toProcess = true;

        <T> Request(
                RelationalPersistentEntity<T> entityType,
                T instance,
                EntityState state,
                PersistentPropertyAccessor<T> accessor) {
            super(entityType);
            this.instance = instance;
            this.state = state;
            this.accessor = accessor;
        }

        @Override
        protected boolean canExecute() {
            return processed && super.canExecute();
        }

        @Override
        protected boolean isDone() {
            return !toProcess || super.isDone();
        }
    }
}
