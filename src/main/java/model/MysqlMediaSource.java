package model;

import lombok.Data;

import java.util.Properties;

@Data
public class MysqlMediaSource extends MediaSource {

    private MeidaType                   meidaType = MeidaType.MYSQL;
    private String                      url;
    private String                      userName;
    private String                      password;
    private Properties                  properties;
    private String                      driver = "com.mysql.jdbc.Driver";
    private String                      encode = "utf8mb4";
}
