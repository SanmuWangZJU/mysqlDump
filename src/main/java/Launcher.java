import configLoader.ConfigFactory;
import exception.DumpException;
import executor.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import model.ColumnPair;
import model.DumpMediaInfo;
import model.RowData;
import process.Extract;
import process.Load;
import service.DataMediaSourceService;
import serviceImpl.MysqlDataMediaSourceServiceImpl;
import utils.SqlUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class Launcher {

    private static final Map<Integer, BlockingQueue<RowData>> productMap = new ConcurrentHashMap<>();
    private static final SqlUtils SQL_UTILS = SqlUtils.getSqlMetaUtils();

    public static void main(String[] args) {
        DumpMediaInfo dumpMediaInfo = ConfigFactory.getDumpMediaInfo();
        DataMediaSourceService mediaSourceService = new MysqlDataMediaSourceServiceImpl();
        DataSource sourceDatasource = mediaSourceService.getDataSource(dumpMediaInfo.getSource());
        DataSource targetDatasource = dumpMediaInfo.isUsingShard()
                ? ((MysqlDataMediaSourceServiceImpl) mediaSourceService).getDataSource()
                : mediaSourceService.getDataSource(dumpMediaInfo.getTarget());
        SqlExecutor sqlExecutor = new SqlExecutor();
        sqlExecutor.setSourceDataSource(sourceDatasource);
        sqlExecutor.setTargetDataSource(targetDatasource);
//        sqlExecutor.build();
        doAfterConfigLoaded(dumpMediaInfo, sqlExecutor);
        for (int i = 0; i < dumpMediaInfo.getMediaPairs().size(); i++) {
            productMap.put(dumpMediaInfo.getMediaPairs().get(i).hashCode(), new LinkedBlockingQueue<>());
        }

        Extract extract = new Extract();
        extract.setMediaPairs(dumpMediaInfo.getMediaPairs());
        extract.setExecutor(sqlExecutor);
        extract.setProductMap(productMap);
        Load load = new Load();
        load.setProductMap(productMap);
        load.setSqlExecutor(sqlExecutor);
        load.setMediaPairs(dumpMediaInfo.getMediaPairs());

        extract.extract();
        load.load();

        sqlExecutor.stop();
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

            List<ResultSet> metaRS = sqlExecutor.executeSelect1RowFromBoth(mediaPair);
            assert metaRS.size() == 2;
            ResultSet sourceRS = metaRS.get(0);
            ResultSet targetRS = metaRS.get(1);

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

            // 拼接成后面需要的全部的column信息
            List<ColumnPair> allColumns = new ArrayList<>();
            allColumns.addAll(mediaPair.getColumnPairs());
            allColumns.addAll(mediaPair.getKeyPairs());
            mediaPair.setAllColumns(allColumns);
        });
    }

}
