package me.lusory.relate.query;

import me.lusory.relate.LcReactiveDataRelationalClient;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.util.Pair;
import org.springframework.lang.NonNull;
import org.springframework.r2dbc.core.DatabaseClient.GenericExecuteSpec;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.BindMarker;
import org.springframework.r2dbc.core.binding.BindMarkers;
import org.springframework.r2dbc.core.binding.BindTarget;
import org.springframework.util.Assert;

import java.util.LinkedList;
import java.util.List;

/**
 * SQL query with binding, for execution on a database client.
 *
 * @param <T> type of query, that can be {@link Select}, {@link Insert}, {@link InsertMultiple},
 *            {@link Update} or {@link Delete}
 */
public class SqlQuery<T> {

    private final LcReactiveDataRelationalClient client;
    private final BindMarkers markers;
    private final List<Pair<BindMarker, Object>> bindings = new LinkedList<>();
    private T query;

    public SqlQuery(LcReactiveDataRelationalClient client) {
        this.client = client;
        markers = client.getDialect().getBindMarkersFactory().create();
    }

    public LcReactiveDataRelationalClient getClient() {
        return client;
    }

    public T getQuery() {
        return query;
    }

    public void setQuery(T query) {
        this.query = query;
    }

    /**
     * Create a bind marker that will be bind to the statement on execution.
     */
    public Expression marker(Object value) {
        BindMarker marker = markers.next();
        bindings.add(Pair.of(marker, value));
        return SQL.bindMarker(marker.getPlaceholder());
    }

    protected String finalizeQuery(String query) {
        return query;
    }

    /**
     * Execute the query.
     */
    public GenericExecuteSpec execute() {
        PreparedOperation<T> operation =
                new PreparedOperation<T>() {
                    @Override
                    public @NonNull
                    T getSource() {
                        return query;
                    }

                    @Override
                    public void bindTo(@NonNull BindTarget target) {
                        for (Pair<BindMarker, Object> binding : bindings) {
                            binding.getFirst().bind(target, binding.getSecond());
                        }
                    }

                    @Override
                    public @NonNull
                    String toQuery() {
                        Assert.notNull(query, "Query must be set");
                        RenderContext renderContext =
                                client.getDataAccess().getStatementMapper().getRenderContext();
                        if (query instanceof InsertMultiple) {
                            return finalizeQuery(((InsertMultiple) query).render(renderContext));
                        }
                        SqlRenderer renderer =
                                renderContext != null
                                        ? SqlRenderer.create(renderContext)
                                        : SqlRenderer.create();
                        if (query instanceof Select) {
                            return finalizeQuery(renderer.render((Select) query));
                        }
                        if (query instanceof Insert) {
                            return finalizeQuery(renderer.render((Insert) query));
                        }
                        if (query instanceof Update) {
                            return finalizeQuery(renderer.render((Update) query));
                        }
                        if (query instanceof Delete) {
                            return finalizeQuery(renderer.render((Delete) query));
                        }
                        throw new IllegalArgumentException(
                                "Unexpected query type: " + query.getClass().getName());
                    }
                };
        return client.getSpringClient().sql(operation);
    }
}
