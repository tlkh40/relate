package net.lecousin.reactive.data.relational.query.operation;

import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Set;

abstract class AbstractProcessor<R extends AbstractProcessor.Request> {

    protected abstract Mono<Void> executeRequests(Operation op);

    protected boolean canExecuteRequest(R request) {
        if (!request.canExecute()) return false;
        request.dependencies.removeIf(Request::isDone);
        return request.dependencies.isEmpty();
    }

    protected Mono<Void> doOperations(Operation op) {
        return executeRequests(op);
    }

    abstract static class Request {
        RelationalPersistentEntity<?> entityType;

        boolean executed = false;

        Set<Request> dependencies = new HashSet<>();

        <T> Request(RelationalPersistentEntity<T> entityType) {
            this.entityType = entityType;
        }

        void dependsOn(Request dependency) {
            if (dependency.dependencies.contains(this))
                throw new IllegalStateException("Cyclic dependency between requests");
            dependencies.add(dependency);
        }

        protected boolean canExecute() {
            return !executed;
        }

        protected boolean isDone() {
            return executed;
        }
    }
}
