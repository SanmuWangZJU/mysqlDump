package serviceImpl;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import configLoader.ConfigFactory;
import io.shardingsphere.shardingjdbc.api.yaml.YamlShardingDataSourceFactory;
import model.MediaSource;
import model.MeidaType;
import model.MysqlMediaSource;
import exception.DumpException;
import service.DataMediaSourceService;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;

public class MysqlDataMediaSourceServiceImpl implements DataMediaSourceService {

    @Override
    public DataSource getDataSource(MediaSource mediaSource) {
        if (mediaSource.getMeidaType().equals(MeidaType.MYSQL)) {
            return createHikariDataSource((MysqlMediaSource) mediaSource);
        } else {
            throw new DumpException("does't support such").addContextValue("media type", mediaSource.getMeidaType());
        }
    }

    /**
     * 如果目标端使用分库分表，则使用这个方法进行初始化
     * @return
     */
    public DataSource getDataSource() {
        DataSource dataSource;
        try {
            dataSource = YamlShardingDataSourceFactory.createDataSource(ConfigFactory.getShardFile());
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            throw new DumpException("fail to get target shard DataSource", e);
        }
        return dataSource;
    }

    private DataSource createHikariDataSource(MysqlMediaSource mysqlMediaSource) {
        HikariDataSource hikariDataSource = new HikariDataSource();

        // metrics info
        MetricRegistry metricRegistry = new MetricRegistry();
        hikariDataSource.setMetricRegistry(metricRegistry);
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
        jmxReporter.start();

        // 设置hikariCP参数
        // 动态参数
        hikariDataSource.setJdbcUrl(mysqlMediaSource.getUrl());
        hikariDataSource.setUsername(mysqlMediaSource.getUserName());
        hikariDataSource.setPassword(mysqlMediaSource.getPassword());
        hikariDataSource.setDriverClassName(mysqlMediaSource.getDriver());
        if (mysqlMediaSource.getEncode().equalsIgnoreCase("utf8mb4")) {
            hikariDataSource.addDataSourceProperty("characterEncoding", "utf8");
            hikariDataSource.setConnectionInitSql("set names utf8mb4");
        } else {
            hikariDataSource.addDataSourceProperty("characterEncoding", mysqlMediaSource.getEncode());
        }
        hikariDataSource.setPoolName(mysqlMediaSource.getUrl());
        hikariDataSource.setAutoCommit(true);

        // 静态参数
        hikariDataSource.setMaximumPoolSize(20); // 连接池最大大小default = 10
        hikariDataSource.setMinimumIdle(10); // 连接池最大大小default = 10
        hikariDataSource.setConnectionTimeout(6000);
        hikariDataSource.setValidationTimeout(60000);
        hikariDataSource.addDataSourceProperty("useSSL", false);
        // settings for preparedStatement
        hikariDataSource.addDataSourceProperty("useServerPrepStmts", "true"); // 启用ps预编译
        hikariDataSource.addDataSourceProperty("cachePrepStmts", "true"); // enable prepared statement cache
        hikariDataSource.addDataSourceProperty("prepStmtCacheSize", "250"); //
        hikariDataSource.addDataSourceProperty("prepStmtCacheSqlLimit", "2048"); // the largest SQL the driver will cache the parsing for
        hikariDataSource.addDataSourceProperty("rewriteBatchedStatements", "true"); // 优化大量的query和insert sql

        hikariDataSource.addDataSourceProperty("cacheResultSetMetadata", "true"); // cache ResultSetMetaData for Statements and PreparedStatements
        hikariDataSource.addDataSourceProperty("cacheServerConfiguration", "true"); //
        hikariDataSource.addDataSourceProperty("elideSetAutoCommits", "true");
        hikariDataSource.addDataSourceProperty("maintainTimeStats", "false");
        hikariDataSource.addDataSourceProperty("zeroDateTimeBehavior", "convertToNull");
        hikariDataSource.addDataSourceProperty("yearIsDateType", "false");
        hikariDataSource.addDataSourceProperty("noDatetimeStringSync", "true");

        return hikariDataSource;
    }
}
