package utils;

import exception.DumpException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class SqlUtils {


    public int getColumnType(ResultSet resultSet, String columnName) {

        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int size = metaData.getColumnCount();
            for (int i = 1; i <= size; i++) {
                if (columnName.equalsIgnoreCase(resultSet.getMetaData().getColumnName(i))) {
                    return resultSet.getMetaData().getColumnType(i);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DumpException("fail to get columnType", e)
                    .addContextValue("columnName", columnName);
        }
        throw new DumpException("can't match column in resultSet");
    }

    public boolean isNumeric(int columnType) {
        boolean res = false;
        switch (columnType) {
            // integer
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.NUMERIC:
            case Types.FLOAT:
            case Types.DECIMAL:
            case Types.BIT:
                res = true;
                break;
        }
        return res;
    }

    public String getColumnValueToString(ResultSet rs, int columnType, String columnName) {
        String res = null;
        try {
            // 如果使用rs.getDouble()等方法时，如果表中值为null，则会返回0，若向表示为null，应该用getString(),取到值后另行判断
//            switch (columnType) {
//                case Types.INTEGER:
//                    res = String.valueOf(rs.getInt(columnName));
//                    break;
//                case Types.DECIMAL:
//                    res = String.valueOf(rs.getDouble(columnName));
//                    break;
//                case Types.TIME:
//                    res = rs.getTime(columnName).toString();
//                    break;
//                case Types.TIMESTAMP:
//                    res = rs.getTimestamp(columnName).toString();
//                    break;
//                default:
//                    res = rs.getString(columnName);
//                    break;
            res = rs.getString(columnName);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return res;
    }



    // singleton
    private SqlUtils() {}

    public static SqlUtils getSqlMetaUtils() {
        return SqlMetaUtilInstancHolder.SQL_META_UTILS;
    }

    private static class SqlMetaUtilInstancHolder{
        private static final SqlUtils SQL_META_UTILS = new SqlUtils();
    }
}
