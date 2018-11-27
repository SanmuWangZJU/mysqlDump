import configLoader.ConfigFactory;
import exception.DumpException;
import executor.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import model.ColumnPair;
import model.DumpMediaInfo;
import model.RowData;
import process.ExtractTask;
import process.LoadTask;
import serviceImpl.MysqlDataMediaSourceServiceImpl;
import utils.SqlBuilder;
import utils.SqlUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class Launcher {

    private static final Map<Integer, BlockingQueue<RowData>> productMap = new ConcurrentHashMap<>();
    private static final SqlUtils SQL_UTILS = SqlUtils.getSqlMetaUtils();

    public static void main(String[] args) {

        DumpMediaInfo dumpMediaInfo = ConfigFactory.getDumpMediaInfo();
        log.debug("dump initial info: \n{}", dumpMediaInfo);
        MysqlDataMediaSourceServiceImpl mediaSourceService = new MysqlDataMediaSourceServiceImpl();
        DataSource sourceDatasource = mediaSourceService.getDataSource(dumpMediaInfo.getSource());
        DataSource targetDatasource = dumpMediaInfo.isUsingShard()
                ? mediaSourceService.getDataSource()
                : mediaSourceService.getDataSource(dumpMediaInfo.getTarget());
        SqlExecutor sqlExecutor = new SqlExecutor(sourceDatasource, targetDatasource);
        ExecutorService taskExecutor = Executors.newFixedThreadPool(2);

        doAfterConfigLoaded(dumpMediaInfo, sqlExecutor);

        for (int i = 0; i < dumpMediaInfo.getMediaPairs().size(); i++) {
            productMap.put(dumpMediaInfo.getMediaPairs().get(i).hashCode(), new LinkedBlockingQueue<>());
        }

        AtomicBoolean extractFinish = new AtomicBoolean(false);
        AtomicBoolean loadFinish = new AtomicBoolean(false);

        ExtractTask extractTask = new ExtractTask();
        extractTask.setExtractBatchSize(dumpMediaInfo.getExtractBatchSize());
        extractTask.setExtractThreadSize(dumpMediaInfo.getExtractThreadSize());
        extractTask.setProductMap(productMap);
        extractTask.setSqlExecutor(sqlExecutor);
        extractTask.setMediaPairs(dumpMediaInfo.getMediaPairs());
        extractTask.setExtractFinish(extractFinish);

        LoadTask loadTask = new LoadTask();
        loadTask.setLoadBatchSize(dumpMediaInfo.getLoadBatchSize());
        loadTask.setProductMap(productMap);
        loadTask.setSqlExecutor(sqlExecutor);
        loadTask.setMediaPairs(dumpMediaInfo.getMediaPairs());
        loadTask.setLoadFinish(loadFinish);

        taskExecutor.submit(extractTask);
        taskExecutor.submit(loadTask);

        while (true) {
            if (loadFinish.get() && extractFinish.get()) {
                sqlExecutor.stop();
                log.info("mydump work finished");
                System.exit(0);
            }
        }
    }

    private static void doAfterConfigLoaded(DumpMediaInfo mediaInfo, SqlExecutor sqlExecutor) {
        mediaInfo.getMediaPairs().forEach(mediaPair -> {
            // 如果只填写了source or target meta 信息，那说明两端meta信息一致，需要fix(这里的meta是指schema.table)
            if (mediaPair.getSourceMeta() == null && mediaPair.getTargetMeta() == null) {
                throw new DumpException("schema and table info can't be empty both in source and target");
            } else if (mediaPair.getSourceMeta() == null && mediaPair.getTargetMeta() != null) {
                mediaPair.getSourceMeta().setSchemaName(mediaPair.getTargetMeta().getSchemaName());
                mediaPair.getSourceMeta().setTableName(mediaPair.getTargetMeta().getTableName());
            } else if (mediaPair.getTargetMeta() == null) {
                mediaPair.getTargetMeta().setSchemaName(mediaPair.getSourceMeta().getSchemaName());
                mediaPair.getTargetMeta().setSchemaName(mediaPair.getSourceMeta().getTableName());
            }
            try (
                    Connection sourceConnection = sqlExecutor.getSourceDataSource().getConnection();
                    Connection targetConnection = sqlExecutor.getTargetDataSource().getConnection()
            ) {

                ResultSet sourceRS = sourceConnection.createStatement().executeQuery(SqlBuilder.getSqlBuilder().getSelect1RowSql(mediaPair.getSourceMeta()));
                ResultSet targetRS = targetConnection.createStatement().executeQuery(SqlBuilder.getSqlBuilder().getSelect1RowSql(mediaPair.getTargetMeta()));

                // 如果只填写了columnName，则说明源端目标端字段名一致
                mediaPair.getColumnPairs().forEach(columnPair -> {
                    if (columnPair.getColumnName() != null) {
                        columnPair.setSourceColumnName(columnPair.getColumnName());
                        columnPair.setTargetColumnName(columnPair.getColumnName());
                    }
                    columnPair.setColumnType(SQL_UTILS.getColumnType(sourceRS, columnPair.getSourceColumnName()));
                    columnPair.setTargetNumeric(SQL_UTILS.isNumeric(SQL_UTILS.getColumnType(targetRS, columnPair.getTargetColumnName())));
                });
                mediaPair.getKeyPairs().forEach(columnPair -> {
                    if (columnPair.getColumnName() != null) {
                        columnPair.setSourceColumnName(columnPair.getColumnName());
                        columnPair.setTargetColumnName(columnPair.getColumnName());
                    }
                    columnPair.setColumnType(SQL_UTILS.getColumnType(sourceRS, columnPair.getSourceColumnName()));
                    columnPair.setTargetNumeric(SQL_UTILS.isNumeric(SQL_UTILS.getColumnType(targetRS, columnPair.getTargetColumnName())));
                });
            } catch (Exception e) {
                e.printStackTrace();
            }

            // 拼接成后面需要的全部的column信息
            List<ColumnPair> allColumns = new ArrayList<>();
            allColumns.addAll(mediaPair.getColumnPairs());
            allColumns.addAll(mediaPair.getKeyPairs());
            mediaPair.setAllColumns(allColumns);
        });
    }

}
