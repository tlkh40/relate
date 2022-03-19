package net.lecousin.reactive.data.relational.repository;

import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactoryBean;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.lang.NonNull;

import java.io.Serializable;

@SuppressWarnings("java:S119") // name of parameter ID
public class LcR2dbcRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
        extends R2dbcRepositoryFactoryBean<T, S, ID> {

    public LcR2dbcRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
        super(repositoryInterface);
    }

    @Override
    protected @NonNull RepositoryFactorySupport getFactoryInstance(@NonNull R2dbcEntityOperations operations) {
        return new LcR2dbcRepositoryFactory(operations);
    }
}
