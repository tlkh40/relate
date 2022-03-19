package net.lecousin.reactive.data.relational;

import io.github.classgraph.*;
import lombok.var;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.lecousin.reactive.data.relational.annotations.ForeignKey;
import net.lecousin.reactive.data.relational.annotations.JoinTable;
import net.lecousin.reactive.data.relational.model.ModelException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.stereotype.Component;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class EntityClassRewriter implements ApplicationRunner {
    private final ByteBuddy byteBuddy = new ByteBuddy();

    @Override
    public void run(ApplicationArguments args) throws Exception {
        ByteBuddyAgent.install();

        final Map<String, DynamicType.Builder<Object>> joinClasses = new HashMap<>();
        Map<String, ClassInfo> entityClasses;
        try (final ScanResult scanResult = new ClassGraph().enableClassInfo().enableFieldInfo().enableAnnotationInfo().scan()) {
            entityClasses = scanResult.getClassesWithAnnotation(Table.class).stream()
                    .map(classInfo -> new AbstractMap.SimpleImmutableEntry<>(classInfo.getName(), classInfo))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

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
            try {
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
                        .defineField("entity1", class1.loadClass(), Modifier.PUBLIC)
                        .annotateType(getJoinFieldAnnotations(columnName1))
                        .defineField("entity2", class2.loadClass(), Modifier.PUBLIC)
                        .annotateType(getJoinFieldAnnotations(columnName2));

                if (field1 != null) {
                    classBuilder = classBuilder.define(
                            getJoinFieldDescription(class1, field1, classBuilder.toTypeDescription(), 1)
                    );
                }
                if (field2 != null) {
                    classBuilder = classBuilder.define(
                            getJoinFieldDescription(class2, field2, classBuilder.toTypeDescription(), 2)
                    );
                }

                joinClasses.put(joinClassName, classBuilder);
            } catch (ModelException e) {
                throw e;
            } catch (Exception e) {
                throw new ModelException(
                        "Error generating join table entity from " + t.getT1().getName() + '#' + t.getT2().getName(),
                        e
                );
            }
        }

        // TODO: add state attribute
        // TODO: persistent fields accessor
        // TODO: lazy methods
        // TODO: process join table getter and setter
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

    private FieldDescription getJoinFieldDescription(ClassInfo classInfo, FieldInfo joinField, TypeDescription joinClass, int linkNumber) {
        return new FieldDescription.Latent(
                joinClass,
                new FieldDescription.Token(
                        joinField.getName() + "_join",
                        Modifier.PUBLIC,
                        TypeDescription.Generic.Builder.parameterizedType(
                                TypeDescription.ForLoadedType.of(Collection.class),
                                joinClass
                        ).build()
                )
        );
    }
}
