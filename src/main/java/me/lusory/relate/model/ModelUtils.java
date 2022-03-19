package me.lusory.relate.model;

import me.lusory.relate.LcReactiveDataRelationalClient;
import me.lusory.relate.annotations.ColumnDefinition;
import me.lusory.relate.annotations.CompositeId;
import me.lusory.relate.annotations.ForeignKey;
import me.lusory.relate.model.LcEntityTypeInfo.ForeignTableInfo;
import me.lusory.relate.query.SqlQuery;
import me.lusory.relate.query.criteria.Criteria;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.core.CollectionFactory;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.*;
import org.springframework.lang.Nullable;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

public class ModelUtils {

    private ModelUtils() {
        // no instance
    }

    /**
     * Check if a property may be null.<br>
     * It cannot be null if:
     *
     * <ul>
     *   <li>the type is a primitive type
     *   <li>this is the id property
     *   <li>this is a foreign key, and it is specified as non optional
     *   <li>this is not a foreign key, and the column definition specifies the column as nullable
     * </ul>
     *
     * @param property
     * @return
     */
    public static boolean isNullable(RelationalPersistentProperty property) {
        if (property.getRawType().isPrimitive()) {
            return false;
        }
        if (property.isIdProperty()) {
            return false;
        }
        ForeignKey fk = property.findAnnotation(ForeignKey.class);
        if (fk != null) {
            return fk.optional();
        }
        ColumnDefinition def = property.findAnnotation(ColumnDefinition.class);
        if (def != null) {
            return def.nullable();
        }
        return true;
    }

    /**
     * Check if a property may be updated.
     *
     * @param property
     * @return
     */
    public static boolean isUpdatable(RelationalPersistentProperty property) {
        if (!property.isWritable()) {
            return false;
        }
        if (property.isIdProperty()) {
            return false;
        }
        ColumnDefinition def = property.findAnnotation(ColumnDefinition.class);
        if (def != null) {
            return def.updatable();
        }
        return true;
    }

    /**
     * Set the foreign table field on the given instance to the given linkedInstance.
     *
     * @param instance       entity having the foreign table field
     * @param linkedInstance entity having the foreign key
     * @param linkedProperty foreign key property
     */
    @SuppressWarnings("java:S3011")
    public static void setReverseLink(
            Object instance, Object linkedInstance, RelationalPersistentProperty linkedProperty) {
        ForeignTableInfo ft =
                LcEntityTypeInfo.get(instance.getClass())
                        .getForeignTableWithFieldForJoinKey(
                                linkedProperty.getName(), linkedInstance.getClass());
        if (ft != null && !ft.isCollection()) {
            try {
                ft.getField().set(instance, linkedInstance);
            } catch (Exception e) {
                throw new ModelAccessException(
                        "Unable to set ForeignTable field "
                                + ft.getField().getName()
                                + " on "
                                + instance.getClass().getSimpleName()
                                + " with value "
                                + linkedInstance,
                        e);
            }
        }
    }

    /**
     * Retrieve all fields from the class and its super classes.
     *
     * @param cl class
     * @return fields
     */
    public static List<Field> getAllFields(Class<?> cl) {
        List<Field> fields = new LinkedList<>();
        getAllFields(cl, fields);
        return fields;
    }

    private static void getAllFields(Class<?> cl, List<Field> fields) {
        if (cl == null) {
            return;
        }
        Collections.addAll(fields, cl.getDeclaredFields());
        getAllFields(cl.getSuperclass(), fields);
    }

    /**
     * Return the identifier for the given entity.
     *
     * @param instance   entity
     * @param entityType entity type
     * @return identifier
     */
    public static Object getRequiredId(
            Object instance,
            RelationalPersistentEntity<?> entityType,
            @Nullable PersistentPropertyAccessor<?> accessor) {
        RelationalPersistentProperty idProperty = entityType.getRequiredIdProperty();
        Object id =
                (accessor != null ? accessor : entityType.getPropertyAccessor(instance))
                        .getProperty(idProperty);
        if (id == null) {
            throw new InvalidEntityStateException(
                    "Entity is supposed to be persisted to database, but it's Id property is null");
        }
        return id;
    }

    /**
     * Check if the given field is a collection.
     *
     * @param field field to check
     * @return true if the field is an array or a Collection
     */
    public static boolean isCollection(Field field) {
        return isCollectionType(field.getType());
    }

    /**
     * Check if the given type is a collection.
     *
     * @param type type to check
     * @return true if the type is an array or implements Collection
     */
    @SuppressWarnings("java:S1126")
    public static boolean isCollectionType(Class<?> type) {
        if (type.isArray()) {
            return !char[].class.equals(type);
        }
        return Collection.class.isAssignableFrom(type);
    }

    /**
     * Return the given object as a collection.
     *
     * @param value the object
     * @return a collection or null
     */
    @SuppressWarnings({"unchecked", "java:S1168"})
    @Nullable
    public static <T> Collection<T> getAsCollection(Object value) {
        if (value instanceof Collection) {
            return (Collection<T>) value;
        }
        if (value.getClass().isArray()) {
            return Arrays.asList((T[]) value);
        }
        return null;
    }

    /**
     * Get the type of elements in a collection field.
     *
     * @param field field
     * @return type of elements
     */
    @Nullable
    public static Class<?> getCollectionType(Field field) {
        if (field.getType().isArray()) {
            return field.getType().getComponentType();
        }
        Type genType = field.getGenericType();
        if (genType instanceof ParameterizedType) {
            return (Class<?>) ((ParameterizedType) genType).getActualTypeArguments()[0];
        }
        return null;
    }

    /**
     * Get the type of elements in a collection field.
     *
     * @param field field
     * @return type of elements
     */
    public static Class<?> getRequiredCollectionType(Field field) {
        if (field.getType().isArray()) {
            return field.getType().getComponentType();
        }
        Type genType = field.getGenericType();
        if (genType instanceof ParameterizedType) {
            return (Class<?>) ((ParameterizedType) genType).getActualTypeArguments()[0];
        }
        throw new MappingException(
                "Field is not a collection: "
                        + field.getDeclaringClass().getName()
                        + "."
                        + field.getName());
    }

    @SuppressWarnings({"unchecked", "java:S3011"})
    public static void addToCollectionField(
            Field field, Object collectionOwnerInstance, Object elementToAdd)
            throws IllegalAccessException {
        if (field.getType().isArray()) {
            Object[] array = (Object[]) field.get(collectionOwnerInstance);
            if (array == null) {
                array = (Object[]) Array.newInstance(field.getType().getComponentType(), 1);
                array[0] = elementToAdd;
                field.set(collectionOwnerInstance, array);
                return;
            }
            if (ArrayUtils.contains(array, elementToAdd)) {
                return;
            }
            Object[] newArray =
                    (Object[])
                            Array.newInstance(field.getType().getComponentType(), array.length + 1);
            System.arraycopy(array, 0, newArray, 0, array.length);
            newArray[array.length] = elementToAdd;
            field.set(collectionOwnerInstance, newArray);
            return;
        }
        Collection<Object> collectionInstance =
                (Collection<Object>) field.get(collectionOwnerInstance);
        if (collectionInstance == null) {
            collectionInstance =
                    CollectionFactory.createCollection(
                            field.getType(), getCollectionType(field), 10);
            field.set(collectionOwnerInstance, collectionInstance);
        }
        if (!collectionInstance.contains(elementToAdd)) {
            collectionInstance.add(elementToAdd);
        }
    }

    @SuppressWarnings("java:S3011")
    public static void removeFromCollectionField(
            Field field, Object collectionOwnerInstance, Object elementToRemove)
            throws IllegalAccessException {
        if (field.getType().isArray()) {
            Object[] array = (Object[]) field.get(collectionOwnerInstance);
            if (array == null) {
                return;
            }
            int index = -1;
            for (int i = 0; i < array.length; ++i) {
                if (array[i] == elementToRemove) {
                    index = i;
                    break;
                }
            }
            if (index < 0) {
                return;
            }
            Object[] newArray =
                    (Object[])
                            Array.newInstance(field.getType().getComponentType(), array.length - 1);
            if (index > 0) {
                System.arraycopy(array, 0, newArray, 0, index);
            }
            if (index < array.length - 1) {
                System.arraycopy(array, index + 1, newArray, index, array.length - index - 1);
            }
            field.set(collectionOwnerInstance, newArray);
            return;
        }
        @SuppressWarnings("unchecked")
        Collection<Object> collectionInstance =
                (Collection<Object>) field.get(collectionOwnerInstance);
        if (collectionInstance == null) {
            return;
        }
        collectionInstance.remove(elementToRemove);
    }

    @SuppressWarnings("java:S3011")
    public static Object getDatabaseValue(
            Object instance,
            RelationalPersistentProperty property,
            LcReactiveDataRelationalClient client) {
        Field f = property.getRequiredField();
        f.setAccessible(true);
        Object value;
        try {
            value = f.get(instance);
        } catch (IllegalAccessException e) {
            throw new ModelAccessException("Unable to get field value", e);
        }
        if (value == null) {
            return null;
        }
        if (property.isAnnotationPresent(ForeignKey.class)) {
            RelationalPersistentEntity<?> e =
                    client.getMappingContext().getRequiredPersistentEntity(value.getClass());
            value = e.getPropertyAccessor(value).getProperty(e.getRequiredIdProperty());
        } else {
            value = client.getSchemaDialect().convertToDataBase(value, property);
        }
        return value;
    }

    public static Object getPersistedDatabaseValue(
            EntityState state,
            RelationalPersistentProperty property,
            MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty>
                    mappingContext) {
        Object value = state.getPersistedValue(property.getName());
        if (value == null) {
            return null;
        }
        if (property.isAnnotationPresent(ForeignKey.class)) {
            RelationalPersistentEntity<?> e =
                    mappingContext.getRequiredPersistentEntity(value.getClass());
            value = e.getPropertyAccessor(value).getProperty(e.getRequiredIdProperty());
        }
        return value;
    }

    public static List<RelationalPersistentProperty> getProperties(
            RelationalPersistentEntity<?> entityType, String... names) {
        ArrayList<RelationalPersistentProperty> list = new ArrayList<>(names.length);
        for (String name : names) {
            list.add(entityType.getRequiredPersistentProperty(name));
        }
        return list;
    }

    public static Object getId(
            RelationalPersistentEntity<?> entityType,
            PersistentPropertyAccessor<?> accessor,
            LcReactiveDataRelationalClient client) {
        if (entityType.hasIdProperty()) {
            return getIdPropertyValue(entityType, accessor);
        }
        if (entityType.isAnnotationPresent(CompositeId.class)) {
            return getIdFromProperties(
                    getProperties(
                            entityType,
                            entityType.getRequiredAnnotation(CompositeId.class).properties()),
                    accessor,
                    client);
        }
        return getIdFromProperties(entityType, accessor, client);
    }

    public static Object getIdPropertyValue(
            RelationalPersistentEntity<?> entityType, PersistentPropertyAccessor<?> accessor) {
        return accessor.getProperty(entityType.getRequiredIdProperty());
    }

    public static CompositeIdValue getIdFromProperties(
            Iterable<RelationalPersistentProperty> properties,
            PersistentPropertyAccessor<?> accessor,
            LcReactiveDataRelationalClient client) {
        CompositeIdValue id = new CompositeIdValue();
        for (RelationalPersistentProperty property : properties) {
            id.add(property.getName(), getDatabaseValue(accessor.getBean(), property, client));
        }
        return id;
    }

    public static Object getId(RelationalPersistentEntity<?> entityType, PropertiesSource source) {
        if (entityType.hasIdProperty()) {
            return getIdPropertyValue(entityType, source);
        }
        if (entityType.isAnnotationPresent(CompositeId.class)) {
            return getIdFromProperties(
                    getProperties(
                            entityType,
                            entityType.getRequiredAnnotation(CompositeId.class).properties()),
                    source);
        }
        return getIdFromProperties(entityType, source);
    }

    public static Object getIdPropertyValue(
            RelationalPersistentEntity<?> entityType, PropertiesSource source) {
        return source.getPropertyValue(entityType.getRequiredIdProperty());
    }

    public static CompositeIdValue getIdFromProperties(
            Iterable<RelationalPersistentProperty> properties, PropertiesSource source) {
        CompositeIdValue id = new CompositeIdValue();
        for (RelationalPersistentProperty property : properties) {
            id.add(property.getName(), source.getPropertyValue(property));
        }
        if (id.isNull()) {
            return null;
        }
        return id;
    }

    public static Condition getConditionOnId(
            SqlQuery<?> query,
            RelationalPersistentEntity<?> entityType,
            PersistentPropertyAccessor<?> accessor,
            LcReactiveDataRelationalClient client) {
        if (entityType.hasIdProperty()) {
            return getConditionOnProperties(
                    query,
                    entityType,
                    Collections.singletonList(entityType.getRequiredIdProperty()),
                    accessor,
                    client);
        }
        if (entityType.isAnnotationPresent(CompositeId.class)) {
            return getConditionOnProperties(
                    query,
                    entityType,
                    getProperties(
                            entityType,
                            entityType.getRequiredAnnotation(CompositeId.class).properties()),
                    accessor,
                    client);
        }
        return getConditionOnProperties(query, entityType, entityType, accessor, client);
    }

    public static Condition getConditionOnProperties(
            SqlQuery<?> query,
            RelationalPersistentEntity<?> entityType,
            Iterable<RelationalPersistentProperty> properties,
            PersistentPropertyAccessor<?> accessor,
            LcReactiveDataRelationalClient client) {
        Iterator<RelationalPersistentProperty> it = properties.iterator();
        Condition condition = null;
        Table table = Table.create(entityType.getTableName());
        do {
            RelationalPersistentProperty property = it.next();
            Object value = getDatabaseValue(accessor.getBean(), property, client);
            Condition propertyCondition =
                    Conditions.isEqual(
                            Column.create(property.getColumnName(), table),
                            value != null ? query.marker(value) : SQL.nullLiteral());
            condition = condition != null ? condition.and(propertyCondition) : propertyCondition;
        } while (it.hasNext());
        return condition;
    }

    public static Criteria getCriteriaOnId(
            String entityName,
            RelationalPersistentEntity<?> entityType,
            PersistentPropertyAccessor<?> accessor,
            LcReactiveDataRelationalClient client) {
        if (entityType.hasIdProperty()) {
            return getCriteriaOnProperties(
                    entityName,
                    Collections.singletonList(entityType.getRequiredIdProperty()),
                    accessor,
                    client);
        }
        if (entityType.isAnnotationPresent(CompositeId.class)) {
            return getCriteriaOnProperties(
                    entityName,
                    getProperties(
                            entityType,
                            entityType.getRequiredAnnotation(CompositeId.class).properties()),
                    accessor,
                    client);
        }
        return getCriteriaOnProperties(entityName, entityType, accessor, client);
    }

    public static Criteria getCriteriaOnProperties(
            String entityName,
            Iterable<RelationalPersistentProperty> properties,
            PersistentPropertyAccessor<?> accessor,
            LcReactiveDataRelationalClient client) {
        Iterator<RelationalPersistentProperty> it = properties.iterator();
        Criteria condition = null;
        do {
            RelationalPersistentProperty property = it.next();
            Object value = getDatabaseValue(accessor.getBean(), property, client);
            Criteria propertyCondition =
                    value != null
                            ? Criteria.property(entityName, property.getName()).is(value)
                            : Criteria.property(entityName, property.getName()).isNull();
            condition = condition != null ? condition.and(propertyCondition) : propertyCondition;
        } while (it.hasNext());
        return condition;
    }

    public static boolean hasCascadeDeleteImpacts(
            Class<?> entityType,
            MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty>
                    mappingContext) {
        LcEntityTypeInfo typeInfo = LcEntityTypeInfo.get(entityType);
        if (!typeInfo.getForeignTables().isEmpty() || !typeInfo.getJoinTables().isEmpty()) {
            return true;
        }
        RelationalPersistentEntity<?> entity =
                mappingContext.getRequiredPersistentEntity(entityType);
        for (RelationalPersistentProperty property : entity) {
            ForeignKey fkAnnotation = property.findAnnotation(ForeignKey.class);
            if (fkAnnotation == null) {
                continue;
            }
            if (fkAnnotation.cascadeDelete()) {
                return true;
            }
            LcEntityTypeInfo foreignInfo = LcEntityTypeInfo.get(property.getActualType());
            ForeignTableInfo ft =
                    foreignInfo.getForeignTableWithFieldForJoinKey(property.getName(), entityType);
            if (ft != null && !ft.getAnnotation().optional()) {
                return true;
            }
        }
        return false;
    }
}
