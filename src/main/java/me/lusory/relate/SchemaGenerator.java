package me.lusory.relate;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public class SchemaGenerator implements ApplicationRunner {
    private final LcReactiveDataRelationalClient lcClient;

    @Override
    public void run(ApplicationArguments args) {
        lcClient.dropCreateSchemaContent(lcClient.buildSchemaFromEntities()).block();
    }
}
