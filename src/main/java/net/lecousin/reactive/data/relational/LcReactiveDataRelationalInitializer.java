package net.lecousin.reactive.data.relational;

import net.lecousin.reactive.data.relational.enhance.ClassPathScanningEntities;
import net.lecousin.reactive.data.relational.enhance.Enhancer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

public class LcReactiveDataRelationalInitializer {

    private static final Log logger = LogFactory.getLog(LcReactiveDataRelationalInitializer.class);

    private static boolean initialized = false;

    private LcReactiveDataRelationalInitializer() {
        // no instance
    }

    public static void init() {
        if (initialized) return;
        initialized = true;
        logger.info("Initializing lc-reactive-data-relational");
        try {
            Config config = loadConfiguration();
            List<String> entities = config.getEntities();
            if (entities.isEmpty()) {
                logger.info(
                        "No entities declared in lc-reactive-data-relational.yaml => scan class path");
                entities = new ArrayList<>(ClassPathScanningEntities.scan());
            }
            Enhancer.enhance(entities);
        } catch (Exception e) {
            logger.error("Error configuring lc-reactive-data-relational", e);
        }
    }

    public static Config loadConfiguration() throws IOException {
        Enumeration<URL> urls =
                LcReactiveDataRelationalInitializer.class
                        .getClassLoader()
                        .getResources("src/test/resources/lc-reactive-data-relational.yaml");
        Config config = new Config();
        while (urls.hasMoreElements()) {
            URL url = urls.nextElement();
            logger.info("Loading configuration from " + url);
            loadConfiguration(url, config);
        }
        return config;
    }

    private static void loadConfiguration(URL url, Config config) {
        try (InputStream input = url.openStream()) {
            Yaml yaml = new Yaml();
            Map<String, Object> root = yaml.load(input);
            if (root.containsKey("entities")) {
                configureEntities(config, "", root.get("entities"));
            }
        } catch (Exception e) {
            logger.error("Unable to read configuration file", e);
        }
    }

    @SuppressWarnings("rawtypes")
    private static void configureEntities(Config config, String prefix, Object value) {
        if (value instanceof String) config.entities.add(prefix + value);
        else if (value instanceof Collection)
            for (Object element : (Collection) value) configureEntities(config, prefix, element);
        else if (value instanceof Map)
            for (Map.Entry entry : ((Map<?, ?>) value).entrySet())
                configureEntities(config, prefix + entry.getKey() + '.', entry.getValue());
        else logger.warn("Unexpected entity package value: " + value);
    }

    public static class Config {
        private final List<String> entities = new LinkedList<>();

        public List<String> getEntities() {
            return entities;
        }
    }
}
