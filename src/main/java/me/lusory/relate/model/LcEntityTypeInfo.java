package me.lusory.relate.model;

import me.lusory.relate.annotations.ForeignTable;
import me.lusory.relate.annotations.JoinTable;
import org.springframework.data.mapping.MappingException;
import org.springframework.lang.Nullable;

import java.lang.reflect.Field;
import java.util.*;

public class LcEntityTypeInfo {

    private static final Map<Class<?>, LcEntityTypeInfo> cache = new HashMap<>();

    private static final String ATTRIBUTE1 = "entity" + "1";
    private static final String ATTRIBUTE2 = "entity" + "2";

    private final Class<?> type;
    private final Field stateField;
    private final Map<String, ForeignTableInfo> foreignTables = new HashMap<>();
    private final Map<String, JoinTableInfo> joinTables = new HashMap<>();

    @SuppressWarnings({"squid:S3011"})
    private LcEntityTypeInfo(Class<?> clazz) throws ModelException {
        type = clazz;
        try {
            stateField = clazz.getDeclaredField("_lcState");
            stateField.setAccessible(true);
        } catch (Exception e) {
            throw new ModelException(
                    "Unable to access to state field for entity class " + clazz.getName());
        }
        List<Field> fields = ModelUtils.getAllFields(clazz);
        for (Field f : fields) {
            ForeignTable ft = f.getAnnotation(ForeignTable.class);
            if (ft != null) {
                foreignTables.put(f.getName(), new ForeignTableInfo(f, ft));
                f.setAccessible(true);
            }
        }
        for (Field f : fields) {
            JoinTable jt = f.getAnnotation(JoinTable.class);
            if (jt != null) {
                JoinTableInfo info = new JoinTableInfo();
                info.field = f;
                info.annotation = jt;
                info.joinForeignTable = foreignTables.get(f.getName() + "_join");
                if (info.joinForeignTable == null) {
                    throw new ModelAccessException(
                            "@JoinTable without corresponding @ForeignTable"); // should never
                }
                // happen with
                // Enhancer
                if (info.joinForeignTable.annotation.joinKey().equals(ATTRIBUTE1)) {
                    info.joinSourceFieldName = ATTRIBUTE1;
                    info.joinTargetFieldName = ATTRIBUTE2;
                } else {
                    info.joinSourceFieldName = ATTRIBUTE2;
                    info.joinTargetFieldName = ATTRIBUTE1;
                }
                joinTables.put(f.getName(), info);
                f.setAccessible(true);
            }
        }
    }

    public static LcEntityTypeInfo get(Class<?> clazz) {
        LcEntityTypeInfo info = cache.get(clazz);
        if (info == null) {
            throw new ModelAccessException(
                    "Unknown entity class "
                            + clazz.getName()
                            + ", known classes are: "
                            + cache.keySet());
        }
        return info;
    }

    public static Collection<Class<?>> getClasses() {
        return cache.keySet();
    }

    public static void setClasses(Collection<Class<?>> classes) throws ModelException {
        for (Class<?> cl : classes) {
            cache.put(cl, new LcEntityTypeInfo(cl));
        }
    }

    public static Collection<Class<?>> addGeneratedJoinTables(Collection<Class<?>> classes) {
        Set<Class<?>> result = new HashSet<>(classes);
        for (Class<?> c : classes) {
            LcEntityTypeInfo info = get(c);
            for (JoinTableInfo joinTable : info.joinTables.values()) {
                Field field = joinTable.joinForeignTable.field;
                Class<?> type;
                if (ModelUtils.isCollection(field)) {
                    type = ModelUtils.getCollectionType(field);
                } else {
                    type = field.getType();
                }
                if (type != null) {
                    result.add(type);
                }
            }
        }
        return result;
    }

    /**
     * Return true if the given field is associated to a @ForeignTable annotation.
     *
     * @param field field
     * @return true if it is a foreign table
     */
    public static boolean isForeignTableField(Field field) {
        LcEntityTypeInfo ti = cache.get(field.getDeclaringClass());
        if (ti == null) {
            return false;
        }
        for (ForeignTableInfo i : ti.foreignTables.values()) {
            if (i.getField().equals(field)) {
                return true;
            }
        }
        return false;
    }

    public Field getStateField() {
        return stateField;
    }

    /**
     * Return the foreign table field having the given join key.
     *
     * @param joinKey    join key
     * @param targetType type of target entity
     * @return the field
     */
    @Nullable
    public Field getForeignTableFieldForJoinKey(String joinKey, Class<?> targetType) {
        ForeignTableInfo i = getForeignTableWithFieldForJoinKey(joinKey, targetType);
        return i != null ? i.getField() : null;
    }

    /**
     * Return the foreign table field having the given join key.
     *
     * @param joinKey    join key
     * @param targetType type of target entity
     * @return the field
     */
    public Field getRequiredForeignTableFieldForJoinKey(String joinKey, Class<?> targetType) {
        return getRequiredForeignTableWithFieldForJoinKey(joinKey, targetType).getField();
    }

    /**
     * Return the foreign table field having the given join key.
     *
     * @param joinKey    join key
     * @param targetType type of target entity
     * @return the field and the foreign table annotation
     */
    @Nullable
    public ForeignTableInfo getForeignTableWithFieldForJoinKey(
            String joinKey, Class<?> targetType) {
        for (Map.Entry<String, ForeignTableInfo> e : foreignTables.entrySet()) {
            ForeignTableInfo ft = e.getValue();
            if (ft.getAnnotation().joinKey().equals(joinKey)) {
                Field field = ft.getField();
                Class<?> fieldType;
                if (ft.isCollection()) {
                    fieldType = ft.getCollectionElementType();
                } else {
                    fieldType = field.getType();
                }
                if (targetType.equals(fieldType)) {
                    return ft;
                }
            }
        }
        return null;
    }

    private String missingForeignTable(String expected, String expectedOn) {
        return "Missing @ForeignTable "
                + expected
                + " '"
                + expectedOn
                + "' in class '"
                + type.getSimpleName()
                + "'";
    }

    /**
     * Return the foreign table field having the given join key.
     *
     * @param joinKey    join key
     * @param targetType type of target entity
     * @return the field and the foreign table annotation
     */
    public ForeignTableInfo getRequiredForeignTableWithFieldForJoinKey(
            String joinKey, Class<?> targetType) {
        ForeignTableInfo i = getForeignTableWithFieldForJoinKey(joinKey, targetType);
        if (i == null) {
            throw new MappingException(missingForeignTable("field with join key", joinKey));
        }
        return i;
    }

    /**
     * Return the foreign table field on the given property.
     *
     * @param propertyName foreign table property
     * @return the field
     */
    @Nullable
    public Field getForeignTableFieldForProperty(String propertyName) {
        ForeignTableInfo i = foreignTables.get(propertyName);
        return i != null ? i.getField() : null;
    }

    /**
     * Return the foreign table info on the given property.
     *
     * @param propertyName foreign table property
     * @return the foreign table
     */
    @Nullable
    public ForeignTableInfo getForeignTableWithFieldForProperty(String propertyName) {
        return foreignTables.get(propertyName);
    }

    /**
     * Return the foreign table field on the given property.
     *
     * @param propertyName foreign table property
     * @return the field
     */
    public Field getRequiredForeignTableFieldForProperty(String propertyName) {
        ForeignTableInfo i = foreignTables.get(propertyName);
        if (i == null) {
            throw new MappingException(missingForeignTable("on property", propertyName));
        }
        return i.getField();
    }

    /**
     * Return the foreign table field on the given property.
     *
     * @param propertyName foreign table property
     * @return the foreign table annotation
     */
    @Nullable
    public ForeignTable getForeignTableForProperty(String propertyName) {
        ForeignTableInfo i = foreignTables.get(propertyName);
        return i != null ? i.getAnnotation() : null;
    }

    /**
     * Return the foreign table field on the given property.
     *
     * @param propertyName foreign table property
     * @return the foreign table annotation
     */
    public ForeignTable getRequiredForeignTableForProperty(String propertyName) {
        ForeignTableInfo i = foreignTables.get(propertyName);
        if (i == null) {
            throw new MappingException(missingForeignTable("on property", propertyName));
        }
        return i.getAnnotation();
    }

    /**
     * Return the list of foreign tables.
     *
     * @return list of fields with their corresponding foreign table annotation
     */
    public Collection<ForeignTableInfo> getForeignTables() {
        return foreignTables.values();
    }

    /**
     * Return join table information on the given property name.
     *
     * @param propertyName property
     * @return join table info
     */
    @Nullable
    public JoinTableInfo getJoinTable(String propertyName) {
        return joinTables.get(propertyName);
    }

    /**
     * Return the list of join tables on this type.
     *
     * @return join tables
     */
    public Collection<JoinTableInfo> getJoinTables() {
        return joinTables.values();
    }

    @SuppressWarnings({"unchecked", "java:S1168", "java:S2259"})
    public <T> Collection<T> getJoinTableElementsForJoinTableClass(
            Object instance, Class<T> joinTableClass) {
        for (JoinTableInfo jti : joinTables.values()) {
            if (ModelUtils.getCollectionType(jti.joinForeignTable.field).equals(joinTableClass)) {
                try {
                    return (Collection<T>) jti.joinForeignTable.field.get(instance);
                } catch (Exception e) {
                    throw new ModelAccessException(
                            "Error accessing join table elements "
                                    + joinTableClass.getName()
                                    + " from "
                                    + instance,
                            e);
                }
            }
        }
        return null;
    }

    public static class ForeignTableInfo {
        private final Field field;
        private final ForeignTable annotation;
        private final boolean isCollection;
        private Class<?> collectionElementType;

        private ForeignTableInfo(Field field, ForeignTable annotation) {
            this.field = field;
            this.annotation = annotation;
            isCollection = ModelUtils.isCollection(field);
            if (isCollection) {
                collectionElementType = ModelUtils.getRequiredCollectionType(field);
            }
        }

        public Field getField() {
            return field;
        }

        public ForeignTable getAnnotation() {
            return annotation;
        }

        public boolean isCollection() {
            return isCollection;
        }

        public Class<?> getCollectionElementType() {
            return collectionElementType;
        }
    }

    public static class JoinTableInfo {
        private Field field;
        private JoinTable annotation;
        private ForeignTableInfo joinForeignTable;
        private String joinSourceFieldName;
        private String joinTargetFieldName;

        public Field getField() {
            return field;
        }

        public JoinTable getAnnotation() {
            return annotation;
        }

        public ForeignTableInfo getJoinForeignTable() {
            return joinForeignTable;
        }

        public String getJoinSourceFieldName() {
            return joinSourceFieldName;
        }

        public String getJoinTargetFieldName() {
            return joinTargetFieldName;
        }
    }
}
