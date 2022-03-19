package net.lecousin.reactive.data.relational.repository;

import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.lang.NonNull;

public class LcR2dbcRepositoryFactory extends R2dbcRepositoryFactory {
    public LcR2dbcRepositoryFactory(R2dbcEntityOperations operations) {
        super(operations);
    }

    @Override
    protected @NonNull Class<?> getRepositoryBaseClass(@NonNull RepositoryMetadata metadata) {
        return LcR2dbcRepositoryImpl.class;
    }
}
