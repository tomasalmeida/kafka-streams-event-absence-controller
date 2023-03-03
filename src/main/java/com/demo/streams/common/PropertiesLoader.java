package com.demo.streams.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class PropertiesLoader {

    public enum CONFIG { DEVICE, STREAMS, HEARTBEAT };
    private static final String PATH_FORMATTER = "src/main/resources/%s.properties";

    public static Properties load(final CONFIG config) throws IOException {

        final String configFile = String.format(PATH_FORMATTER, config.toString().toLowerCase());

        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (final InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}
