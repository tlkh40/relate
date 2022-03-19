package net.lecousin.reactive.data.relational.query.criteria;

import net.lecousin.reactive.data.relational.annotations.ForeignKey;
import net.lecousin.reactive.data.relational.model.ModelUtils;
import net.lecousin.reactive.data.relational.query.SqlQuery;
import net.lecousin.reactive.data.relational.query.criteria.Criteria.And;
import net.lecousin.reactive.data.relational.query.criteria.Criteria.Or;
import net.lecousin.reactive.data.relational.query.criteria.Criteria.PropertyOperand;
import net.lecousin.reactive.data.relational.query.criteria.Criteria.PropertyOperation;
import net.lecousin.reactive.data.relational.schema.dialect.RelationalDatabaseSchemaDialect.SqlFunction;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class CriteriaSqlBuilder implements CriteriaVisitor<Condition> {

    protected Map<String, RelationalPersistentEntity<?>> entitiesByAlias;
    protected Map<String, Table> tablesByAlias;
    protected SqlQuery<?> query;

    public CriteriaSqlBuilder(
            Map<String, RelationalPersistentEntity<?>> entitiesByAlias,
            Map<String, Table> tablesByAlias,
            SqlQuery<?> query) {
        this.entitiesByAlias = entitiesByAlias;
        this.tablesByAlias = tablesByAlias;
        this.query = query;
    }

    @Override
    public Condition visit(And and) {
        return and.getLeft().accept(this).and(and.getRight().accept(this));
    }

    @Override
    public Condition visit(Or or) {
        return or.getLeft().accept(this).or(or.getRight().accept(this));
    }

    @SuppressWarnings({"incomplete-switch", "java:S1301", "java:S131"})
    @Override
    public Condition visit(PropertyOperation op) {
        RelationalPersistentEntity<?> entity = entitiesByAlias.get(op.getLeft().getEntityName());
        RelationalPersistentProperty property =
                entity.getRequiredPersistentProperty(op.getLeft().getPropertyName());

        Expression left = toExpression(op.getLeft());

        switch (op.getOperator()) {
            case IS_NULL:
                return Conditions.isNull(left);
            case IS_NOT_NULL:
                return Conditions.isNull(left).not();
        }

        if (op.getValue() instanceof Collection) {
            Collection<?> value = (Collection<?>) op.getValue();
            List<Expression> expressions = new ArrayList<>(value.size());
            for (Object v : value) expressions.add(toExpression(v, property));

            switch (op.getOperator()) {
                case IN:
                    return Conditions.in(left, expressions);
                case NOT_IN:
                    return Conditions.in(left, expressions).not();
                default:
                    throw new InvalidCriteriaException(
                            "Unexpected operator " + op.getOperator() + " on a collection");
            }
        }

        Object rightValue = op.getValue();
        if (property.isAnnotationPresent(ForeignKey.class)
                && rightValue != null
                && property.getType().isAssignableFrom(rightValue.getClass())) {
            // if foreign key, we need to use the id instead of the object
            MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty>
                    context = query.getClient().getMappingContext();
            RelationalPersistentEntity<?> foreignEntity =
                    context.getRequiredPersistentEntity(property.getType());
            rightValue =
                    ModelUtils.getId(
                            foreignEntity,
                            foreignEntity.getPropertyAccessor(rightValue),
                            query.getClient());
        }
        Expression right = toExpression(rightValue, property);
        switch (op.getOperator()) {
            case EQUALS:
                return Conditions.isEqual(left, right);
            case NOT_EQUALS:
                return Conditions.isNotEqual(left, right);
            case GREATER_THAN:
                return Conditions.isGreater(left, right);
            case GREATER_THAN_OR_EQUAL:
                return Conditions.isGreaterOrEqualTo(left, right);
            case LESS_THAN:
                return Conditions.isLess(left, right);
            case LESS_THAN_OR_EQUAL:
                return Conditions.isLessOrEqualTo(left, right);
            case LIKE:
                return Conditions.like(left, right);
            case NOT_LIKE:
                return Conditions.like(left, right).not();
            default:
                throw new InvalidCriteriaException("Unexpected operator " + op.getOperator());
        }
    }

    protected Expression toExpression(Object value, RelationalPersistentProperty property) {
        if (value instanceof PropertyOperand) return toExpression((PropertyOperand) value);
        return query.marker(
                query.getClient().getSchemaDialect().convertToDataBase(value, property));
    }

    protected Expression toExpression(PropertyOperand propertyOperand) {
        RelationalPersistentEntity<?> rightEntity =
                entitiesByAlias.get(propertyOperand.getEntityName());
        Expression result = Column.create(
                rightEntity
                        .getRequiredPersistentProperty(propertyOperand.getPropertyName())
                        .getColumnName(),
                tablesByAlias.get(propertyOperand.getEntityName()));
        for (SqlFunction fct : propertyOperand.getFunctionsToApply())
            result = query.getClient().getSchemaDialect().applyFunctionTo(fct, result);
        return result;
    }
}
