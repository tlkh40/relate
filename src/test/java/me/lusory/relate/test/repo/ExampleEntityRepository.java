package me.lusory.relate.test.repo;

import me.lusory.relate.test.repo.model.ExampleEntity1;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ExampleEntityRepository extends ReactiveCrudRepository<ExampleEntity1, Long> {
}
