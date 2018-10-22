package process;

import exception.DumpException;
import executor.SqlExecutor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import model.ColumnData;
import model.ColumnPair;
import model.MediaPair;
import model.RowData;
import utils.SqlBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.BlockingQueue;

@Slf4j
@Data
public class Extract {
    private List<MediaPair> mediaPairs;
    private SqlExecutor executor;
    private Map<Integer, BlockingQueue<RowData>> productMap;
    private boolean needOrder = false;

    private List<String> buildExtractSql(MediaPair mediaPair) {
        return SqlBuilder.getSqlBuilder().getSelectSQL(mediaPair);
    }

    public void extract() {
        mediaPairs.parallelStream().forEach(mediaPair -> {
            List<String> selectSqls = buildExtractSql(mediaPair);
            selectSqls.parallelStream().forEach(selectSql->{
                ResultSet rs = executor.executeSelectFromSource(selectSql);
                try {
                    BlockingQueue queue = productMap.get(mediaPair.hashCode());
                    while (rs.next()) {
                        List<ColumnData> columnDatas = new ArrayList<>();
                        for (ColumnPair columnPair : mediaPair.getAllColumns()) {
                            // 使用rs.getDouble()等columnType本身的方法时，如果表中值为null，则会返回0；若向表示为null，应该用getString(),取到值后另行判断
                            String columnValue = rs.getString(columnPair.getSourceColumnName());
                            columnDatas.add(ColumnData.builder()
                                    .columnName(columnPair.getTargetColumnName())
                                    .columnValue(columnValue)
                                    .isKey(mediaPair.getKeyPairs().contains(columnPair))
                                    .build());
                        }
                        RowData data = RowData.builder().mediaPair(mediaPair).value(columnDatas).build();
    //                    log.info(data.toString());
                        queue.offer(data);
                    }
                    queue.offer(RowData.builder().end(true).build());
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DumpException("error occur while extract from source", e)
                            .addContextValue("source", mediaPair.getSourceMeta())
                            .addContextValue("extract sql", selectSql);
                }
            });
        });
    }

}
