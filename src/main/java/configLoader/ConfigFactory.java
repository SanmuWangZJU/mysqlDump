package configLoader;

import model.DumpMediaInfo;

import java.io.File;
import java.util.Objects;

public class ConfigFactory {

    public static DumpMediaInfo getDumpMediaInfo() {
        String path = Objects.requireNonNull(ConfigFactory.class.getClassLoader().getResource("conf.yaml")).getPath();
        Object object = YamlConfigLoader.loadConfig(path, DumpMediaInfo.class);
        assert object instanceof DumpMediaInfo;
        return (DumpMediaInfo) object;
    }

    public static File getShardFile() {
        String path = ConfigFactory.class.getClassLoader().getResource("shard.yaml").getFile();
        return new File(path);
    }
}
