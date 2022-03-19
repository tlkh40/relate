package me.lusory.relate.schema;

import me.lusory.relate.LcReactiveDataRelationalClient;
import me.lusory.relate.annotations.*;
import me.lusory.relate.model.ModelUtils;
import me.lusory.relate.annotations.ColumnDefinition;
import me.lusory.relate.annotations.CompositeId;
import me.lusory.relate.annotations.ForeignKey;
import me.lusory.relate.annotations.GeneratedValue;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Pair;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.*;

public class SchemaBuilderFromEntities {

    protected LcReactiveDataRelationalClient client;
    protected RelationalDatabaseSchema schema = new RelationalDatabaseSchema();

    public SchemaBuilderFromEntities(LcReactiveDataRelationalClient client) {
        this.client = client;
    }

    public RelationalDatabaseSchema getSchema() {
        return schema;
    }

    public RelationalDatabaseSchema build(Collection<Class<?>> entities) {
        for (Class<?> entity : entities) {
            schema.add(buildTable(entity));
            addSequences(entity);
        }
        for (Class<?> entity : entities) {
            addForeignKeys(entity);
        }
        return schema;
    }

    protected String getTableName(RelationalPersistentEntity<?> entityType) {
        return entityType.getTableName().toSql(client.getDialect().getIdentifierProcessing());
    }

    protected String getColumnName(RelationalPersistentProperty property) {
        return property.getColumnName().toSql(client.getDialect().getIdentifierProcessing());
    }

    protected Table buildTable(Class<?> entity) {
        RelationalPersistentEntity<?> entityType =
                client.getMappingContext().getRequiredPersistentEntity(entity);
        Table table = new Table(getTableName(entityType));
        for (RelationalPersistentProperty property : entityType) {
            try {
                table.add(buildColumn(property));
            } catch (Exception e) {
                throw new MappingException(
                        "Error building schema for entity "
                                + entityType.getName()
                                + " on property "
                                + property.getName(),
                        e);
            }
        }
        CompositeId compositeId = entityType.findAnnotation(CompositeId.class);
        if (compositeId != null) {
            Index index = new Index(compositeId.indexName());
            index.setUnique(true);
            for (String propertyName : compositeId.properties()) {
                RelationalPersistentProperty property =
                        entityType.getRequiredPersistentProperty(propertyName);
                index.addColumn(
                        property.getColumnName()
                                .toSql(client.getDialect().getIdentifierProcessing()));
            }
            table.add(index);
        }
        List<me.lusory.relate.annotations.Index> indexes = new LinkedList<>();
        me.lusory.relate.annotations.Index indexAnnotation =
                entityType.findAnnotation(
                        me.lusory.relate.annotations.Index.class);
        if (indexAnnotation != null) {
            indexes.add(indexAnnotation);
        }
        Indexes indexesAnnotation =
                entityType.findAnnotation(
                        Indexes.class);
        if (indexesAnnotation != null) {
            Collections.addAll(indexes, indexesAnnotation.value());
        }
        for (me.lusory.relate.annotations.Index i : indexes) {
            Index index = new Index(i.name());
            index.setUnique(i.unique());
            for (String propertyName : i.properties()) {
                RelationalPersistentProperty property =
                        entityType.getRequiredPersistentProperty(propertyName);
                index.addColumn(getColumnName(property));
            }
            table.add(index);
        }
        return table;
    }

    protected Column buildColumn(RelationalPersistentProperty property) {
        Column col =
                new Column(
                        property.getColumnName()
                                .toSql(client.getDialect().getIdentifierProcessing()));
        if (property.isAnnotationPresent(Id.class)) {
            col.setPrimaryKey(true);
        }
        col.setNullable(ModelUtils.isNullable(property));
        GeneratedValue generated = property.findAnnotation(GeneratedValue.class);
        if (generated != null) {
            if (GeneratedValue.Strategy.AUTO_INCREMENT.equals(generated.strategy())) {
                col.setAutoIncrement(true);
            } else if (GeneratedValue.Strategy.RANDOM_UUID.equals(generated.strategy())) {
                col.setRandomUuid(true);
            }
        }
        Class<?> type = property.getType();
        if (property.isAnnotationPresent(ForeignKey.class)) {
            RelationalPersistentEntity<?> entity =
                    client.getMappingContext().getRequiredPersistentEntity(type);
            RelationalPersistentProperty idProperty = entity.getRequiredIdProperty();
            type = idProperty.getType();
        }
        ColumnDefinition def = property.findAnnotation(ColumnDefinition.class);
        col.setType(client.getSchemaDialect().getColumnType(col, type, def));
        return col;
    }

    protected void addForeignKeys(Class<?> entity) {
        RelationalPersistentEntity<?> entityType =
                client.getMappingContext().getRequiredPersistentEntity(entity);
        Iterator<RelationalPersistentProperty> keys =
                entityType.getPersistentProperties(ForeignKey.class).iterator();
        if (!keys.hasNext()) {
            return;
        }
        Table table = schema.getTable(getTableName(entityType));
        do {
            RelationalPersistentProperty fkProperty = keys.next();
            Column fkColumn = table.getColumn(getColumnName(fkProperty));
            RelationalPersistentEntity<?> foreignType =
                    client.getMappingContext().getRequiredPersistentEntity(fkProperty.getType());
            RelationalPersistentProperty foreignId = foreignType.getRequiredIdProperty();
            Table foreignTable = schema.getTable(getTableName(foreignType));
            Column foreignColumn = foreignTable.getColumn(getColumnName(foreignId));
            fkColumn.setForeignKeyReferences(Pair.of(foreignTable, foreignColumn));
        } while (keys.hasNext());
    }

    protected void addSequences(Class<?> entity) {
        RelationalPersistentEntity<?> entityType =
                client.getMappingContext().getRequiredPersistentEntity(entity);
        for (RelationalPersistentProperty property :
                entityType.getPersistentProperties(GeneratedValue.class)) {
            GeneratedValue annotation = property.getRequiredAnnotation(GeneratedValue.class);
            if (annotation.strategy().equals(GeneratedValue.Strategy.SEQUENCE)) {
                Assert.isTrue(
                        StringUtils.hasText(annotation.sequence()),
                        "Sequence name must be specified");
                try {
                    schema.getSequence(annotation.sequence());
                    // already defined
                } catch (NoSuchElementException e) {
                    schema.add(new Sequence(annotation.sequence()));
                }
            }
        }
    }
}
