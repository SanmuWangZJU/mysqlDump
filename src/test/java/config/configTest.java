package config;

import configLoader.ConfigFactory;
import model.DumpMediaInfo;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class configTest {

    @Test
    public void configGetTest() {
        DumpMediaInfo dumpMediaInfo = ConfigFactory.getDumpMediaInfo();
        log.info(dumpMediaInfo.toString());
    }


}
