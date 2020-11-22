package net.lecousin.reactive.data.relational.query.operation;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableObject;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.util.Pair;

import net.lecousin.reactive.data.relational.LcReactiveDataRelationalClient;
import net.lecousin.reactive.data.relational.annotations.ForeignKey;
import net.lecousin.reactive.data.relational.annotations.ForeignTable;
import net.lecousin.reactive.data.relational.enhance.EntityState;
import net.lecousin.reactive.data.relational.model.ModelAccessException;
import net.lecousin.reactive.data.relational.model.ModelUtils;
import reactor.core.publisher.Mono;

class DeleteProcessor extends AbstractProcessor<DeleteProcessor.DeleteRequest> {

	/** Leaf entities that can be deleted without the need to load them. */
	private Map<RelationalPersistentEntity<?>, Criteria> toDeleteWithoutLoading = new HashMap<>();
	
	static class DeleteRequest extends AbstractProcessor.Request {

		DeleteRequest(RelationalPersistentEntity<?> entityType, Object instance, EntityState state, PersistentPropertyAccessor<?> accessor) {
			super(entityType, instance, state, accessor);
		}
		
	}
	
	@Override
	protected DeleteRequest createRequest(Object instance, EntityState state, RelationalPersistentEntity<?> entity, PersistentPropertyAccessor<?> accessor) {
		return new DeleteRequest(entity, instance, state, accessor);
	}
	
	@Override
	protected boolean checkRequest(Operation op, DeleteRequest request) {
		return request.state.isPersisted();
	}

	@Override
	@SuppressWarnings("java:S3011")
	protected void processForeignKey(
		Operation op, DeleteRequest request,
		RelationalPersistentProperty fkProperty, ForeignKey fkAnnotation,
		Field foreignTableField, ForeignTable foreignTableAnnotation
	) {
		if (ModelUtils.isCollection(foreignTableField)) {
			// remove from collection if loaded
			removeFromForeignTableCollection(request, fkProperty, foreignTableField);
			return;
		}
		
		if (foreignTableAnnotation.optional()) {
			// set to null if loaded
			if (request.state.isLoaded() && !request.state.isFieldModified(fkProperty.getName())) {
				Object foreignInstance = request.accessor.getProperty(fkProperty);
				if (foreignInstance != null) {
					try {
						foreignTableField.set(foreignInstance, null);
					} catch (Exception e) {
						throw new ModelAccessException("Cannot set foreign table field", e);
					}
				}
			}
			return;
		}
		
		// delete where id in (foreign key values)
		if (request.state.isLoaded()) {
			deleteForeignKeyInstance(op, request, request.state.getPersistedValue(fkProperty.getName()));
			return;
		}

		op.loader.load(request.entityType, request.instance, loaded -> deleteForeignKeyInstance(op, request, request.accessor.getProperty(fkProperty)));
	}
	
	private static void removeFromForeignTableCollection(DeleteRequest request, RelationalPersistentProperty fkProperty, Field foreignTableField) {
		if (request.state.isLoaded() && !request.state.isFieldModified(fkProperty.getName())) {
			Object foreignInstance = request.accessor.getProperty(fkProperty);
			if (foreignInstance != null) {
				try {
					ModelUtils.removeFromCollectionField(foreignTableField, foreignInstance, request.instance);
				} catch (Exception e) {
					throw new ModelAccessException("Cannot remove instance from collection field", e);
				}
			}
		}
	}
	
	private void deleteForeignKeyInstance(Operation op, DeleteRequest request, Object foreignInstance) {
		if (foreignInstance == null)
			return;
		DeleteRequest deleteForeign = addToProcess(op, foreignInstance, null, null, null);
		request.dependsOn(deleteForeign);
	}
	
	@Override
	protected void processForeignTableField(
		Operation op, DeleteRequest request,
		Field foreignTableField, ForeignTable foreignTableAnnotation, MutableObject<?> foreignFieldValue, boolean isCollection,
		RelationalPersistentEntity<?> foreignEntity, RelationalPersistentProperty fkProperty, ForeignKey fkAnnotation
	) {
		if (fkAnnotation.optional() && fkAnnotation.onForeignKeyDeleted().equals(ForeignKey.OnForeignDeleted.SET_TO_NULL)) {
			// update to null
			Object instId = ModelUtils.getRequiredId(request.instance, request.entityType, request.accessor);
			op.updater.update(foreignEntity, fkProperty, instId, null);
			if (foreignFieldValue != null) {
				Object foreignInstance = foreignFieldValue.getValue();
				if (foreignInstance != null) {
					EntityState.get(foreignInstance, op.lcClient, foreignEntity).setPersistedField(foreignInstance, fkProperty.getField(), null, true);
				}
			}
			return;
		}
		
		//delete
		if (foreignFieldValue != null && !request.state.isFieldModified(foreignTableField.getName())) {
			// foreign loaded
			Object foreignInstance = foreignFieldValue.getValue();
			if (foreignInstance == null)
				return; // no link
			if (ModelUtils.isCollection(foreignTableField)) {
				for (Object o : ModelUtils.getAsCollection(foreignFieldValue.getValue()))
					addToProcess(op, o, foreignEntity, null, null);
			} else {
				addToProcess(op, foreignFieldValue.getValue(), foreignEntity, null, null);
			}
		} else {
			// foreign not loaded
			Object instId = ModelUtils.getRequiredId(request.instance, request.entityType, request.accessor);
			if (!hasOtherLinks(op, foreignEntity.getType(), foreignTableAnnotation.joinKey())) {
				// can do delete where fk in (ids)
				deleteWithoutLoading(foreignEntity, Criteria.where(fkProperty.getName()).is(instId));
			} else {
				// need to retrieve the entity from database, then process them to be deleted
				op.loader.retrieve(foreignEntity, fkProperty, instId, loaded -> addToProcess(op, loaded, foreignEntity, null, null));
			}
		}
	}

	
	private void deleteWithoutLoading(RelationalPersistentEntity<?> entity, Criteria criteria) {
		Criteria c = toDeleteWithoutLoading.get(entity);
		if (c == null)
			toDeleteWithoutLoading.put(entity, criteria);
		else
			toDeleteWithoutLoading.put(entity, c.or(criteria));
	}
	
	private static boolean hasOtherLinks(Operation op, Class<?> entityType, String otherThanField) {
		for (Pair<Field, ForeignTable> p : ModelUtils.getForeignTables(entityType)) {
			if (!p.getFirst().getName().equals(otherThanField))
				return true;
		}
		RelationalPersistentEntity<?> entity = op.lcClient.getMappingContext().getRequiredPersistentEntity(entityType);
		for (RelationalPersistentProperty prop : entity) {
			if (!prop.getName().equals(otherThanField) && prop.isAnnotationPresent(ForeignKey.class))
				return true;
		}
		return false;
	}
	
	@Override
	protected Mono<Void> doOperations(Operation op) {
		Mono<Void> executeRequests = super.doOperations(op);
		Mono<Void> deleteWithoutLoading = doDeleteWithoutLoading(op);
		if (executeRequests != null) {
			if (deleteWithoutLoading != null)
				return Mono.when(executeRequests, deleteWithoutLoading);
			return executeRequests;
		}
		return deleteWithoutLoading;
	}
	
	@Override
	protected Mono<Void> doRequests(Operation op, RelationalPersistentEntity<?> entityType, List<DeleteRequest> requests) {
		Criteria criteria = entityType.hasIdProperty() ? createCriteriaOnIds(entityType, requests) : createCriteriaOnProperties(entityType, requests);
		return op.lcClient.getSpringClient()
			.delete().from(entityType.getType())
			.matching(criteria)
			.then()
			.doOnSuccess(v -> op.toCall(() -> deleteDone(entityType, requests)));
	}
	
	private static Criteria createCriteriaOnIds(RelationalPersistentEntity<?> entityType, List<DeleteRequest> requests) {
		List<Object> ids = new ArrayList<>(requests.size());
		for (DeleteRequest request : requests) {
			Object id = entityType.getPropertyAccessor(request.instance).getProperty(entityType.getRequiredIdProperty());
			ids.add(id);
		}
		if (LcReactiveDataRelationalClient.logger.isDebugEnabled())
			LcReactiveDataRelationalClient.logger.debug("Delete " + entityType.getType().getName() + " with ids " + ids);
		return Criteria.where(entityType.getRequiredIdProperty().getName()).in(ids);
	}
	
	private static Criteria createCriteriaOnProperties(RelationalPersistentEntity<?> entityType, List<DeleteRequest> requests) {
		Criteria criteria = Criteria.empty();
		for (DeleteRequest request : requests) {
			Criteria c = Criteria.empty();
			for (RelationalPersistentProperty property : entityType) {
				Object value = request.accessor.getProperty(property);
				if (value == null)
					c = c.and(Criteria.where(property.getName()).isNull());
				else
					c = c.and(Criteria.where(property.getName()).is(value));
			}
			criteria = criteria.or(c);
		}
		return criteria;
	}
	
	private static void deleteDone(RelationalPersistentEntity<?> entityType, List<DeleteRequest> done) {
		for (DeleteRequest request : done) {
			// change the state of the entity instance
			request.state.deleted();
			// set id to null
			RelationalPersistentProperty idProperty = entityType.getIdProperty();
			if (idProperty != null && !idProperty.getType().isPrimitive())
				entityType.getPropertyAccessor(request.instance).setProperty(idProperty, null);
		}
	}

	private Mono<Void> doDeleteWithoutLoading(Operation op) {
		List<Mono<Void>> calls = new LinkedList<>();
		Map<RelationalPersistentEntity<?>, Criteria> map = toDeleteWithoutLoading;
		toDeleteWithoutLoading = new HashMap<>();
		for (Map.Entry<RelationalPersistentEntity<?>, Criteria> entity : map.entrySet()) {
			if (LcReactiveDataRelationalClient.logger.isDebugEnabled())
				LcReactiveDataRelationalClient.logger.debug("Delete " + entity.getKey().getType().getName() + " where " + entity.getValue());
			calls.add(
				op.lcClient.getSpringClient().delete().from(entity.getKey().getType()).matching(entity.getValue())
				.then()
			);
		}
		if (calls.isEmpty())
			return null;
		return Mono.when(calls);
	}
	
}
