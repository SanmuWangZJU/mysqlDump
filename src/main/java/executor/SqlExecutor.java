package executor;

import com.zaxxer.hikari.HikariDataSource;
import exception.DumpException;
import io.shardingsphere.shardingjdbc.jdbc.core.datasource.ShardingDataSource;
import lombok.AllArgsConstructor;
import lombok.Data;
import model.ColumnData;
import model.ColumnPair;
import model.MediaPair;
import model.RowData;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@Data
public class SqlExecutor {
    private DataSource sourceDataSource;
    private DataSource targetDataSource;

    public void executeInsertToTarget(String sql, List<RowData> rowDatas) {

    }

    public boolean executeInsertToTargetWithPrepareStatement(String sql, MediaPair mediaPair, List<RowData> rowDatas) {
        try (
                Connection connection = getTargetDataSource().getConnection()
                ){
            PreparedStatement ps = connection.prepareStatement(sql);
            for (RowData data : rowDatas) {
                setPSParameters(ps, mediaPair, data);
                ps.addBatch();
            }
            int[] affectRows = ps.executeBatch();
            return affectRows.length == rowDatas.size();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DumpException("fail update data to target DB.", e)
                    .addContextValue("sql", sql);
        }

    }

    private void setPSParameters(PreparedStatement ps, MediaPair mediaPair, RowData data) throws SQLException {
        List<ColumnData> dataList = new ArrayList<>();
        for (ColumnPair pair : mediaPair.getAllColumns()) {
            dataList.add(getColumnDataByName(data.getValue(), pair.getTargetColumnName()));
        }

//        System.out.println(ps.getParameterMetaData().getParameterCount());
        for (int i = 0; i < dataList.size(); i++) {
            // psIndex 从1开始计数
            ps.setObject(i+1, dataList.get(i).getColumnValue());
        }
    }

    private ColumnData getColumnDataByName(List<ColumnData> columnDatas, String columnName) {
        ColumnData data = null;
        for (ColumnData tmp : columnDatas) {
            if (tmp.getColumnName().equals(columnName)) {
                data = tmp;
                break;
            }
        }
        if (data == null) {
            throw new DumpException("can't find specified columnData")
                    .addContextValue("needColumn", columnName)
                    .addContextValue("all column Datas", columnDatas);
        }
        return data;
    }


    public void stop() {
        if (getSourceDataSource() != null) {
            ((HikariDataSource) getSourceDataSource()).close();
        }
        if (getTargetDataSource() != null) {
            if (getTargetDataSource() instanceof HikariDataSource) {
                ((HikariDataSource) getTargetDataSource()).close();
            } else if (getTargetDataSource() instanceof ShardingDataSource) {
                ((ShardingDataSource) getTargetDataSource()).close();
            } else {
                // ignore the error while shutting down datasource
            }
        }
    }

}
