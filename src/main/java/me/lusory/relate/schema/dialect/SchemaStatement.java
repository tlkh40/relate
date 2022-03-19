package me.lusory.relate.schema.dialect;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SchemaStatement {

    private final String sql;
    private final Set<SchemaStatement> dependencies = new HashSet<>();
    private final Set<SchemaStatement> doNotExecuteTogether = new HashSet<>();

    public SchemaStatement(String sql) {
        this.sql = sql;
    }

    public void addDependency(SchemaStatement dependsOn) {
        dependencies.add(dependsOn);
    }

    public void removeDependency(SchemaStatement statement) {
        dependencies.remove(statement);
    }

    public boolean hasDependency() {
        return !dependencies.isEmpty();
    }

    public void doNotExecuteTogether(SchemaStatement statement) {
        doNotExecuteTogether.add(statement);
    }

    public boolean canExecuteWith(List<SchemaStatement> statements) {
        return doNotExecuteTogether.stream().noneMatch(statements::contains);
    }

    public String getSql() {
        return sql;
    }
}
