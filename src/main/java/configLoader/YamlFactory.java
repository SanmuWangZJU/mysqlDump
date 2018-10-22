package configLoader;

import org.yaml.snakeyaml.Yaml;

class YamlFactory {
    private YamlFactory() { }
    private static Yaml yamlInstance = new Yaml();

    static Yaml getYamlInstance() {
        return yamlInstance;
    }
}
