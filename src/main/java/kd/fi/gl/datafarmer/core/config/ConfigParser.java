package kd.fi.gl.datafarmer.core.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.Map;

public class ConfigParser {
    private static Logger log = LoggerFactory.getLogger(ConfigParser.class);

    //((Map)objectMap.get("core")).get("college")
    private static Map<String, Object> propertyMap = null;
    static {
        String configPath = System.getProperty("appConfigFile");
        BufferedReader br;
        if (null == configPath || configPath.isEmpty()) {
            br = new BufferedReader(new InputStreamReader(ConfigParser.class.getClassLoader().getResourceAsStream("app.yaml")));
        } else {
            try {
                br = new BufferedReader(new FileReader(configPath));
            } catch (FileNotFoundException e) {
                log.error(e.getMessage(), log);
                throw new RuntimeException(e);
            }
        }

        Yaml yaml = new Yaml();
        propertyMap = (Map<String, Object>) yaml.load(br);
    }

    public static Object getKey(String... items) {
        if (items == null || items.length == 0) {
            throw new RuntimeException("must specific property path");
        }
        Object value = propertyMap;
        for (int i = 0; i < items.length; i++) {
            value = ((Map<String, Object>)value).get(items[i]);
        }
        return value;
    }
}
