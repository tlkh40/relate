package me.lusory.relate.test;

import me.lusory.relate.repository.LcR2dbcRepositoryFactoryBean;
import me.lusory.relate.test.repo.ExampleEntityRepository;
import me.lusory.relate.test.repo.model.ExampleEntity1;
import me.lusory.relate.test.repo.model.ExampleEntity2;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;

@SpringBootTest(properties = {
        "spring.r2dbc.url=r2dbc:h2:mem:///testdb;DB_CLOSE_DELAY=-1;",
        "spring.r2dbc.username=sa"
})
@SpringBootApplication(scanBasePackages = "me.lusory.relate.test")
@EnableR2dbcRepositories(repositoryFactoryBeanClass = LcR2dbcRepositoryFactoryBean.class)
public class OneToOneTest {
    @Autowired
    private ExampleEntityRepository exampleEntityRepository;

    @Test
    void persist() {
        exampleEntityRepository.save(
                ExampleEntity1.builder()
                        .test("test")
                        .build()
        ).doOnNext(System.out::println).block();
    }
}
