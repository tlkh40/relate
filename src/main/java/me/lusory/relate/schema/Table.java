package me.lusory.relate.schema;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class Table {

    private final String name;
    private final List<Column> columns = new LinkedList<>();
    private final List<Index> indexes = new LinkedList<>();

    public Table(String name) {
        this.name = name;
    }

    public void add(Column col) {
        columns.add(col);
    }

    public void add(Index index) {
        indexes.add(index);
    }

    public String getName() {
        return name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    public Column getColumn(String name) {
        for (Column col : columns) {
            if (col.getName().equals(name)) {
                return col;
            }
        }
        throw new NoSuchElementException("Column <" + name + "> in table <" + name + ">");
    }
}
