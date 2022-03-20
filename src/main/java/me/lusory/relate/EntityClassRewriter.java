package me.lusory.relate;

import io.github.classgraph.*;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.var;
import me.lusory.relate.annotations.ColumnDefinition;
import me.lusory.relate.annotations.ForeignKey;
import me.lusory.relate.annotations.ForeignTable;
import me.lusory.relate.annotations.JoinTable;
import me.lusory.relate.model.*;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.annotation.AnnotationList;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.*;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.pool.TypePool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringApplicationRunListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EntityClassRewriter implements SpringApplicationRunListener {
    private static final Advice SETTER_METHOD_ADVICE = Advice.to(SetterMethodAdvice.class);
    private static final MethodDelegation ENTITY_LOADED_METHOD_DELEGATION = MethodDelegation.to(EntityLoadedMethodDelegate.class);
    private static final MethodDelegation LOAD_ENTITY_METHOD_DELEGATION = MethodDelegation.to(LoadEntityMethodDelegate.class);

    private final ByteBuddy byteBuddy = new ByteBuddy();
    private final Map<String, Map<String, JoinTableInfo>> joinTableFields = new HashMap<>();

    public EntityClassRewriter() {
        // dummy testing constructor
    }

    private EntityClassRewriter(SpringApplication application, String[] args) {
        // dummy constructor for superinterface
    }

    @Override
    @SneakyThrows
    public void contextPrepared(ConfigurableApplicationContext context) {
        ByteBuddyAgent.install();

        final Map<String, DynamicType.Builder<Object>> joinClasses = new HashMap<>();
        Map<String, ClassInfo> entityClasses;
        try (final ScanResult scanResult = new ClassGraph().enableClassInfo().enableFieldInfo().enableMethodInfo().enableAnnotationInfo().scan()) {
            entityClasses = scanResult.getClassesWithAnnotation(Table.class).stream()
                    .collect(Collectors.toMap(ClassInfo::getName, Function.identity()));
        }

        final TypePool contextTypePool = TypePool.Default.of(Thread.currentThread().getContextClassLoader());

        // process join table fields and create join classes
        final LinkedList<Tuple4<ClassInfo, FieldInfo, AnnotationInfo, ClassInfo>> joins = new LinkedList<>(); // variable type needs to be LinkedList
        for (final ClassInfo classInfo : entityClasses.values()) {
            for (final FieldInfo fieldInfo : classInfo.getDeclaredFieldInfo()) {
                if (!fieldInfo.hasAnnotation(JoinTable.class)) {
                    continue;
                }
                try {
                    final AnnotationInfo joinTable = fieldInfo.getAnnotationInfo(JoinTable.class);
                    final TypeSignature type = fieldInfo.getTypeSignatureOrTypeDescriptor();
                    if (
                            !(type instanceof ClassRefTypeSignature)
                            || !((ClassRefTypeSignature) type).getClassInfo().implementsInterface(Set.class)
                    ) {
                        throw new ModelException(
                                "Attribute " + classInfo.getName() + "#" + fieldInfo.getName() + " annotated with @JoinTable must be a Set"
                        );
                    }
                    final ClassRefTypeSignature classRef = (ClassRefTypeSignature) type;
                    final ClassInfo elemType = classRef.getTypeArguments().stream()
                            .filter(typeArgument -> typeArgument.getWildcard() == TypeArgument.Wildcard.NONE && typeArgument.getTypeSignature() instanceof ClassRefTypeSignature)
                            .map(typeArgument -> ((ClassRefTypeSignature) typeArgument.getTypeSignature()).getClassInfo())
                            .findFirst()
                            .orElseThrow(() -> new ModelException(
                                    "Unexpected type for @JoinTable field, must be a Set with 1 type argument: " + classInfo.getName() + '#' + fieldInfo.getName()
                            ));
                    final ClassInfo target = entityClasses.get(elemType.getName());
                    if (target == null) {
                        throw new ModelException(
                                "Unexpected collection element type " + elemType + " for @JoinTable field: " + classInfo.getName() + '#' + fieldInfo.getName()
                        );
                    }
                    joins.add(Tuples.of(classInfo, fieldInfo, joinTable, target));
                } catch (ModelException e) {
                    throw e;
                } catch (Exception e) {
                    throw new ModelException(
                            "Error getting @JoinTable field info for " + classInfo.getName() + '#' + fieldInfo.getName(),
                            e
                    );
                }
            }
        }

        // create joins
        while (!joins.isEmpty()) {
            Tuple4<ClassInfo, FieldInfo, AnnotationInfo, ClassInfo> t = joins.removeFirst();
            // create join table
            final List<Tuple4<ClassInfo, FieldInfo, AnnotationInfo, ClassInfo>> targetJoins = new LinkedList<>();
            for (final Tuple4<ClassInfo, FieldInfo, AnnotationInfo, ClassInfo> tt : joins) {
                if (!tt.getT1().equals(t.getT4())) {
                    continue;
                }
                if (!tt.getT3().getParameterValues().getValue("tableName").equals(t.getT3().getParameterValues().getValue("tableName"))) {
                    continue;
                }
                if (((String) tt.getT3().getParameterValues().getValue("joinProperty")).length() > 0
                        && !(tt.getT3().getParameterValues().getValue("joinProperty").equals(t.getT2().getName()))) {
                    continue;
                }
                if (((String) t.getT3().getParameterValues().getValue("joinProperty")).length() > 0
                        && !t.getT3().getParameterValues().getValue("joinProperty").equals(tt.getT2().getName())) {
                    continue;
                }
                targetJoins.add(tt);
            }

            if (targetJoins.size() > 1) {
                throw new ModelException(
                        "@JoinTable on field " + t.getT1().getName() + '#' + t.getT2().getName() + " is ambiguous"
                );
            }

            ClassInfo class1;
            ClassInfo class2;
            FieldInfo field1 = null;
            FieldInfo field2 = null;
            String columnName1 = "";
            String columnName2 = "";
            if (t.getT1().getName().compareTo(t.getT4().getName()) < 0) {
                class1 = t.getT1();
                class2 = t.getT4();
                field1 = t.getT2();
                columnName1 = (String) t.getT3().getParameterValues().getValue("columnName");
            } else {
                class1 = t.getT4();
                class2 = t.getT1();
                field2 = t.getT2();
                columnName2 = (String) t.getT3().getParameterValues().getValue("columnName");
            }

            String tableName;

            if (targetJoins.isEmpty()) {
                if (((String) t.getT3().getParameterValues().getValue("joinProperty")).length() > 0) {
                    throw new ModelException(
                            "@JoinTable on field "
                                    + t.getT1().getName()
                                    + '#'
                                    + t.getT2().getName()
                                    + " refers to a property ("
                                    + t.getT3().getParameterValues().getValue("joinProperty")
                                    + ") that does not exist on "
                                    + t.getT4().getName()
                    );
                }
                tableName = (String) t.getT3().getParameterValues().getValue("tableName");
            } else {
                Tuple4<ClassInfo, FieldInfo, AnnotationInfo, ClassInfo> tt = targetJoins.get(0);
                joins.remove(tt);
                tableName = (String) tt.getT3().getParameterValues().getValue("tableName");
                if (class1.equals(tt.getT1())) {
                    field1 = tt.getT2();
                    columnName1 = (String) tt.getT3().getParameterValues().getValue("columnName");
                } else {
                    field2 = tt.getT2();
                    columnName2 = (String) tt.getT3().getParameterValues().getValue("columnName");
                }
            }

            if (tableName.isEmpty()) {
                final AnnotationInfo t1 = class1.getAnnotationInfo(Table.class);
                final AnnotationInfo t2 = class2.getAnnotationInfo(Table.class);
                String name1 = (String) t1.getParameterValues().getValue("value");
                String name2 = (String) t2.getParameterValues().getValue("value");
                if (name1.isEmpty()) {
                    name1 = class1.getSimpleName();
                }
                if (name2.isEmpty()) {
                    name2 = class2.getSimpleName();
                }
                tableName = name1 + '_' + name2 + "_JOIN";
            }

            String joinClassName = class1.getPackageName();
            if (joinClassName != null) {
                joinClassName = joinClassName + ".JoinEntity_" + class1.getSimpleName() + '_' + class2.getSimpleName();
            } else {
                joinClassName = "JoinEntity_" + class1.getSimpleName() + '_' + class2.getSimpleName();
            }

            var classBuilder = byteBuddy.subclass(Object.class)
                    .name(joinClassName)
                    .annotateType(
                            AnnotationDescription.Builder.ofType(Table.class)
                                    .define("value", tableName)
                                    .build()
                    )
                    .defineField("entity1", contextTypePool.describe(class1.getName()).resolve(), Modifier.PUBLIC)
                    .annotateField(getJoinFieldAnnotations(columnName1))
                    .defineField("entity2", contextTypePool.describe(class2.getName()).resolve(), Modifier.PUBLIC)
                    .annotateField(getJoinFieldAnnotations(columnName2));

            final TypeDescription builderType = classBuilder.toTypeDescription();
            if (field1 != null) {
                joinTableFields.computeIfAbsent(class1.getName(), c -> new HashMap<>()).put(
                        field1.getName(),
                        JoinTableInfo.builder()
                                .joinClassName(joinClassName)
                                .linkNumber(1)
                                .build()
                );
                classBuilder = classBuilder.define(
                        getJoinFieldDescription(field1, builderType, 1)
                );
            }
            if (field2 != null) {
                joinTableFields.computeIfAbsent(class2.getName(), c -> new HashMap<>()).put(
                        field2.getName(),
                        JoinTableInfo.builder()
                                .joinClassName(joinClassName)
                                .linkNumber(2)
                                .build()
                );
                classBuilder = classBuilder.define(
                        getJoinFieldDescription(field2, builderType, 2)
                );
            }

            joinClasses.put(joinClassName, classBuilder);
        }

        final Set<DynamicType.Builder<?>> classBuilders = new HashSet<>();
        // process the entity classes
        for (final ClassInfo classInfo : entityClasses.values()) {
            classBuilders.add(processEntityClassBuilder(byteBuddy.subclass(contextTypePool.describe(classInfo.getName()).resolve())));
        }
        // process the join table class builders
        for (final DynamicType.Builder<Object> builder : joinClasses.values()) {
            classBuilders.add(processEntityClassBuilder(builder));
        }
        // process join table accessors
        for (final Map.Entry<String, Map<String, JoinTableInfo>> classEntry : joinTableFields.entrySet()) {
            for (final Map.Entry<String, JoinTableInfo> propertyEntry : classEntry.getValue().entrySet()) {
                classBuilders.add(
                        byteBuddy.subclass(contextTypePool.describe(classEntry.getKey()).resolve())
                                .method(ElementMatchers.isGetter(propertyEntry.getKey()))
                                .intercept(MethodDelegation.to(new JoinGetterMethodDelegate(propertyEntry)))
                                .method(ElementMatchers.isSetter(propertyEntry.getKey()))
                                .intercept(MethodDelegation.to(new JoinSetterMethodDelegate(propertyEntry)))
                );
            }
        }

        LcEntityTypeInfo.setClasses(classBuilders.stream().map(e -> e.make().load(Thread.currentThread().getContextClassLoader()).getLoaded()).collect(Collectors.toList()));
    }

    private List<AnnotationDescription> getJoinFieldAnnotations(String columnName) {
        final List<AnnotationDescription> annotations = new ArrayList<>();
        annotations.add(
                AnnotationDescription.Builder.ofType(ForeignKey.class)
                        .define("optional", false)
                        .build()
        );
        if (columnName.length() > 0) {
            annotations.add(
                    AnnotationDescription.Builder.ofType(Column.class)
                            .define("value", columnName)
                            .build()
            );
        }
        return annotations;
    }

    private FieldDescription getJoinFieldDescription(FieldInfo joinField, TypeDescription joinClass, int linkNumber) {
        return new FieldDescription.Latent(
                joinClass,
                joinField.getName() + "_join",
                Modifier.PUBLIC,
                TypeDescription.Generic.Builder.parameterizedType(
                        TypeDescription.ForLoadedType.of(Collection.class),
                        joinClass
                ).build(),
                Collections.singletonList(
                        AnnotationDescription.Builder.ofType(ForeignTable.class)
                                .define("joinKey", "entity" + linkNumber)
                                .build()
                )
        );
    }

    private boolean isPersistent(FieldDescription field) {
        final AnnotationList annotations = field.getDeclaredAnnotations();
        if (annotations.isAnnotationPresent(Transient.class)
                || annotations.isAnnotationPresent(Autowired.class)
                || annotations.isAnnotationPresent(Value.class)) {
            return false;
        }
        return annotations.isAnnotationPresent(Id.class)
                || annotations.isAnnotationPresent(Column.class)
                || annotations.isAnnotationPresent(ColumnDefinition.class)
                || annotations.isAnnotationPresent(Version.class)
                || annotations.isAnnotationPresent(ForeignKey.class);
    }

    private <T> DynamicType.Builder<T> processEntityClassBuilder(DynamicType.Builder<T> builder) {
        // add state attribute
        builder = builder.defineField("_rlState", EntityState.class, Modifier.PUBLIC)
                .annotateField(AnnotationDescription.Builder.ofType(Transient.class).build());

        for (final FieldDescription fieldDescription : builder.toTypeDescription().getDeclaredFields()) {
            if (!isPersistent(fieldDescription)) {
                continue;
            }

            // persistent fields accessor
            builder = builder.visit(SETTER_METHOD_ADVICE.on(ElementMatchers.isSetter(fieldDescription.getName()))); // TODO: perhaps expand this to account for type
        }

        final TypeDescription builderType = builder.toTypeDescription();

        // lazy methods
        for (final MethodDescription methodDescription : builderType.getDeclaredMethods()) {
            if (methodDescription.getName().startsWith("lazyGet")) {
                final String propertyName = StringUtils.uncapitalize(methodDescription.getName().substring(7));
                final FieldDescription field = builderType.getDeclaredFields().stream()
                        .filter(f -> f.getName().equals(propertyName))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("Backing field " + propertyName + " for lazy accessor not found"));

                final AnnotationDescription foreignTable = field.getDeclaredAnnotations().ofType(ForeignTable.class);
                if (foreignTable != null) {
                    if (field.getType().isArray() || field.getType().asErasure().isAssignableFrom(Collection.class)) {
                        builder = builder.method(method -> method.equals(methodDescription))
                                .intercept(MethodDelegation.to(new LazyForeignTableCollectionGetterMethodDelegate(propertyName, foreignTable.getValue("joinKey").resolve(String.class))));
                    } else {
                        builder = builder.method(method -> method.equals(methodDescription))
                                .intercept(MethodDelegation.to(new LazyForeignTableGetterMethodDelegate(propertyName, foreignTable.getValue("joinKey").resolve(String.class))));
                    }
                    continue;
                }

                final AnnotationDescription joinTable = field.getDeclaredAnnotations().ofType(JoinTable.class);
                if (joinTable != null) {
                    builder = builder.method(method -> method.equals(methodDescription))
                            .intercept(MethodDelegation.to(new LazyJoinTableGetterMethodDelegate(propertyName, joinTableFields.get(builderType.getName()).get(propertyName).linkNumber)));
                    continue;
                }

                final AnnotationDescription foreignKey = field.getDeclaredAnnotations().ofType(ForeignKey.class);
                if (foreignKey != null) {
                    builder = builder.method(method -> method.equals(methodDescription))
                            .intercept(MethodDelegation.to(new LazyForeignKeyGetterMethodDelegate(propertyName)));
                } else {
                    builder = builder.method(method -> method.equals(methodDescription))
                            .intercept(MethodDelegation.to(new LazyForeignKeyFieldGetterMethodDelegate(propertyName)));
                }
            }
        }

        return builder.method(ElementMatchers.named("entityLoaded").and(ElementMatchers.takesNoArguments()).and(ElementMatchers.returns(boolean.class)))
                .intercept(ENTITY_LOADED_METHOD_DELEGATION)
                .defineMethod("loadEntity", Mono.class, Modifier.PUBLIC)
                .intercept(LOAD_ENTITY_METHOD_DELEGATION);
    }

    // advices and method delegates

    public static class SetterMethodAdvice {
        @Advice.OnMethodEnter
        public static void intercept(
                @Advice.FieldValue("_rlState") EntityState _rlState,
                @Advice.Origin("#p") String backingField,
                @Advice.Argument(0) Object newValue
        ) {
            _rlState.fieldSet(backingField, newValue);
        }
    }

    public static class EntityLoadedMethodDelegate {
        @RuntimeType
        public static boolean intercept(@FieldValue("_rlState") EntityState _rlState) {
            return _rlState != null && _rlState.isLoaded();
        }
    }

    public static class LoadEntityMethodDelegate {
        @RuntimeType
        public static Mono<?> intercept(@This Object instance, @FieldValue("_rlState") EntityState _rlState) {
            return _rlState.load(instance);
        }
    }

    @RequiredArgsConstructor
    public static class JoinGetterMethodDelegate {
        private final Map.Entry<String, JoinTableInfo> propertyEntry;

        @RuntimeType
        @SneakyThrows
        public Object intercept(
                @FieldValue Object backingField,
                @This Object instance,
                @Origin Class<?> instrumentedType
        ) {
            if (backingField != null) {
                return backingField;
            }
            final Field joinField = instrumentedType.getDeclaredField(propertyEntry.getKey() + "_join");
            joinField.setAccessible(true);
            final Object joinFieldI = joinField.get(instance);
            if (joinFieldI != null) {
                final Collection<?> newCollection = new JoinTableCollectionToTargetCollection<>(
                        instance,
                        (Collection<?>) joinFieldI,
                        propertyEntry.getValue().joinClassName,
                        propertyEntry.getValue().linkNumber
                );
                final Field propertyField = instrumentedType.getDeclaredField(propertyEntry.getKey());
                propertyField.setAccessible(true);
                propertyField.set(instance, newCollection);
                return newCollection;
            }
            return null;
        }
    }

    @RequiredArgsConstructor
    public static class JoinSetterMethodDelegate {
        private final Map.Entry<String, JoinTableInfo> propertyEntry;

        @SneakyThrows
        public <T> void intercept(
                @This Object instance,
                @Origin Class<?> instrumentedType,
                @Argument(0) @RuntimeType Collection<T> arg
        ) {
            final Field joinField = instrumentedType.getDeclaredField(propertyEntry.getKey() + "_join");
            joinField.setAccessible(true);
            joinField.set(
                    instance,
                    new JoinTableCollectionFromTargetCollection<>(
                            instance,
                            (Collection<?>) joinField.get(instance),
                            Set.class.isAssignableFrom(arg.getClass()) ? (Set<T>) arg : new HashSet<>(arg),
                            propertyEntry.getValue().joinClassName,
                            propertyEntry.getValue().linkNumber
                    )
            );
        }
    }

    @RequiredArgsConstructor
    public static class LazyForeignTableCollectionGetterMethodDelegate {
        private final String propertyName;
        private final String joinKey;

        @RuntimeType
        public Flux<?> intercept(@This Object instance, @FieldValue("_rlState") EntityState _rlState) {
            return _rlState.lazyGetForeignTableCollectionField(instance, propertyName, joinKey);
        }
    }

    @RequiredArgsConstructor
    public static class LazyForeignTableGetterMethodDelegate {
        private final String propertyName;
        private final String joinKey;

        @RuntimeType
        public Mono<?> intercept(@This Object instance, @FieldValue("_rlState") EntityState _rlState) {
            return _rlState.lazyGetForeignTableField(instance, propertyName, joinKey);
        }
    }

    @RequiredArgsConstructor
    public static class LazyJoinTableGetterMethodDelegate {
        private final String propertyName;
        private final int linkNumber;

        @RuntimeType
        public Flux<?> intercept(@This Object instance, @FieldValue("_rlState") EntityState _rlState) {
            return _rlState.lazyGetJoinTableField(instance, propertyName, linkNumber);
        }
    }

    @RequiredArgsConstructor
    public static class LazyForeignKeyGetterMethodDelegate {
        private final String propertyName;

        @RuntimeType
        @SneakyThrows
        public Mono<?> intercept(@This Object instance, @Origin Class<?> instrumentedType) {
            // if ForeignKey, ensure it is loaded
            Method foreignKeyGetter;
            try {
                foreignKeyGetter = instrumentedType.getDeclaredMethod("get" + StringUtils.capitalize(propertyName));
            } catch (NoSuchMethodException ignored) {
                foreignKeyGetter = instrumentedType.getDeclaredMethod(propertyName);
            }
            final Object value = foreignKeyGetter.invoke(instance);
            return value != null ? ((EntityState) value.getClass().getDeclaredField("_rlState").get(value)).load(value) : Mono.empty();
        }
    }

    @RequiredArgsConstructor
    public static class LazyForeignKeyFieldGetterMethodDelegate {
        private final String propertyName;

        @RuntimeType
        public Mono<?> intercept(@This Object instance, @FieldValue("_rlState") EntityState _rlState) {
            return _rlState.load(instance)
                    .map(_rlState.getFieldMapper(instance, propertyName));
        }
    }

    @Builder
    private static class JoinTableInfo {
        private String joinClassName;
        private int linkNumber;
    }
}
