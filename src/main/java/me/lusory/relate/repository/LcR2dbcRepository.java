package me.lusory.relate.repository;

import me.lusory.relate.LcReactiveDataRelationalClient;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.repository.NoRepositoryBean;

@NoRepositoryBean
@SuppressWarnings("java:S119") // name of parameter ID
public interface LcR2dbcRepository<T, ID> extends R2dbcRepository<T, ID> {

    LcReactiveDataRelationalClient getLcClient();
}
