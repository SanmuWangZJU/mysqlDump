package configLoader;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

@Slf4j
public class YamlConfigLoader {

    private static YamlConfigLoader yamlConfigLoaderInstance = getYamlConfigLoaderInstance();
    private static Yaml yaml = YamlFactory.getYamlInstance();

    static Object loadConfig(String resourceURI, Class className) {
        Object result = null;
        try (
                FileInputStream fileInputStream = new FileInputStream(new File((resourceURI)))
        ){
            result = yaml.loadAs(fileInputStream, className);
        } catch (FileNotFoundException ignored) {
            log.warn("can't find config path for " + resourceURI + ", ignore this if this URI is not used for sharding rules");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    static Optional<Object> loadConfig(String resourceName) throws FileNotFoundException {
        Object result = null;
        try (
                FileInputStream fileInputStream = new FileInputStream(new File((resourceName)))
        ){
            result = yaml.load(fileInputStream);
        } catch (FileNotFoundException e) {
            // throw to upper loader to judge whether ignore the fileNotFoundException or not
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Optional.ofNullable(result);
    }

    static Optional<Object> loadConfigByGivenString(String configToBeLoaded) {
        Object result = null;
        result = yaml.load(configToBeLoaded);
        return Optional.ofNullable(result);
    }

    static Optional<Object> loadConfigByGivenString(String configToBeLoaded, Class className) {
        Object result = null;
        if (StringUtils.isNotBlank(configToBeLoaded)) {
            result = yaml.loadAs(configToBeLoaded, className);
        }
        return Optional.ofNullable(result);
    }

    private YamlConfigLoader() {
    }

    private static YamlConfigLoader getYamlConfigLoaderInstance() {
        return instanceHolder.instance;
    }

    private static class instanceHolder {
        private static final YamlConfigLoader instance = new YamlConfigLoader();
    }
}
