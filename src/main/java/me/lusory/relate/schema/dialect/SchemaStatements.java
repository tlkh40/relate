package me.lusory.relate.schema.dialect;

import me.lusory.relate.LcReactiveDataRelationalClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class SchemaStatements {

    private static final Log LOGGER = LogFactory.getLog(SchemaStatements.class);

    private final List<SchemaStatement> statements = new LinkedList<>();

    private static String log(String sql) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql);
        }
        return sql;
    }

    private static void log(SchemaStatement s, Throwable error) {
        LOGGER.error("Error executing " + s.getSql(), error);
    }

    public void add(SchemaStatement statement) {
        statements.add(statement);
    }

    private List<SchemaStatement> peekReadyStatements() {
        List<SchemaStatement> ready = new LinkedList<>();
        synchronized (statements) {
            for (Iterator<SchemaStatement> it = statements.iterator(); it.hasNext(); ) {
                SchemaStatement s = it.next();
                if (s.hasDependency() || !s.canExecuteWith(ready)) {
                    continue;
                }
                ready.add(s);
                it.remove();
            }
        }
        return ready;
    }

    private void done(SchemaStatement done) {
        synchronized (statements) {
            for (SchemaStatement s : statements) {
                s.removeDependency(done);
            }
        }
    }

    public Mono<Void> execute(LcReactiveDataRelationalClient client) {
        return Flux.just("").expand(s -> execute(client, peekReadyStatements())).then();
    }

    private Flux<String> execute(
            LcReactiveDataRelationalClient client, List<SchemaStatement> statements) {
        return Flux.fromIterable(statements)
                .subscribeOn(Schedulers.parallel())
                .publishOn(Schedulers.parallel())
                .parallel()
                .runOn(Schedulers.parallel())
                .flatMap(
                        s ->
                                client.getSpringClient()
                                        .sql(log(s.getSql()))
                                        .fetch()
                                        .rowsUpdated()
                                        .doOnError(e -> log(s, e))
                                        .thenReturn(s))
                .sequential(1)
                .doOnNext(this::done)
                .map(s -> "");
    }

    public void print(PrintStream target) {
        while (!statements.isEmpty()) {
            for (SchemaStatement statement : peekReadyStatements()) {
                target.print(statement.getSql());
                target.println(";");
                done(statement);
            }
        }
    }
}
