package net.lecousin.reactive.data.relational;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class EntityClassRewriter implements ApplicationRunner {
    private final ByteBuddy byteBuddy = new ByteBuddy();

    @Override
    public void run(ApplicationArguments args) throws Exception {
        ByteBuddyAgent.install();

        List<Class<?>> entityClasses;
        try (final ScanResult scanResult = new ClassGraph().enableClassInfo().enableAnnotationInfo().scan()) {
            entityClasses = scanResult.getClassesWithAnnotation(Table.class).loadClasses();
        }

        // TODO: the enhancing
    }
}
