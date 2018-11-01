package utils;

import exception.DumpException;
import model.ColumnPair;
import model.MediaPair;
import model.MetaInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 构建所需的SQL，目前只考虑全量数据，所以只有insert
 */
public class SqlBuilder {
    private static final String SELECT_SQL_TEMPLATE = "SELECT %s FROM %s";
    private static final String SELECT_LIMIT_SQL = "%s LIMIT %d, %d";
    private static final String SELECT_ONE_ROW_SQL = "SELECT * FROM %s LIMIT 0, 0"; // 用于获取结构
    private static final String INSERT_SQL = "INSERT INTO %s (%s) values( %s )";
    private static final String INSERT_UPDATE_SQL = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s";
    private static final String VALUE_TEMP = "%s VALUES(%s)";
    private static final String DUPLICATE_KEY_TEMP = "%s = VALUES(%s)";
    private static final String BACK_QUOTE = "`";
    private static final String DOT = ".";
    private static final String COMMA = ",";
    private static final String APOSTROPHE = "'";


    /**
     * 源端读数据的SQL
     * @param mediaPair
     * @return
     */
    public List<String> getSelectSQL(MediaPair mediaPair){

        List<String> columnNamesToSelect = new ArrayList<>();
        mediaPair.getColumnPairs().forEach(columnPair -> columnNamesToSelect.add(buildNameForSql(columnPair.getSourceColumnName())));
        mediaPair.getKeyPairs().forEach(columnPair -> columnNamesToSelect.add(buildNameForSql(columnPair.getSourceColumnName())));
        String targetSql = StringUtils.join(columnNamesToSelect, COMMA);
        List<String> schemaNames = new InlineExpressionParser(mediaPair.getSourceMeta().getSchemaName()).evaluate();
        List<String> tableNames = new InlineExpressionParser(mediaPair.getSourceMeta().getTableName()).evaluate();
        List<String> fromInfos = new ArrayList<>();
        schemaNames.forEach(schemaName-> tableNames.forEach(tableName->fromInfos.add(buildSchemaAndTableName(schemaName, tableName))));
        return fromInfos.stream().map(fromInfo -> String.format(SELECT_SQL_TEMPLATE, targetSql, fromInfo)).collect(Collectors.toList());
    }

    /**
     * 源端读数据的SQL，加limit（防止数据量过大的情况，便于分批处理）
     * @param mediaPair
     * @param start
     * @param batchSize
     * @return
     */
    public List<String> getSelectSQLWithLimit(MediaPair mediaPair, int start, int batchSize) {

        List<String> baseInfos = getSelectSQL(mediaPair);
        return baseInfos.stream().map(baseInfo->String.format(SELECT_LIMIT_SQL, baseInfo, start, batchSize)).collect(Collectors.toList());
    }

    /**
     * 用于获取表的meta信息
     * @param metaInfo
     * @return
     */
    public String getSelect1RowSql(MetaInfo metaInfo) {
        try {
            String schemaName = new InlineExpressionParser(metaInfo.getSchemaName()).evaluate().get(0);
            String tableName = new InlineExpressionParser(metaInfo.getTableName()).evaluate().get(0);
            return String.format(SELECT_ONE_ROW_SQL, buildSchemaAndTableName(schemaName, tableName));
        } catch (Exception e) {
            throw new DumpException("fail to get meta info of source or target table", e)
                    .addContextValue("DB meta info", metaInfo);
        }
    }

    public String getInsertSql(MediaPair mediaPair) {
        return null;
    }

    /**
     * 对于向分库分表同步，不支持分片键的更新
     * @param mediaPair
     * @return
     */
    public String getInsertUpdatePSSqlForTarget(MediaPair mediaPair) {
        String res;
        List<ColumnPair> columnPairs = mediaPair.getColumnPairs();
        res = String.format(INSERT_UPDATE_SQL,
                buildSchemaAndTableName(mediaPair.getTargetMeta().getSchemaName(), mediaPair.getTargetMeta().getTableName()),
                buildColumnNames(mediaPair.getAllColumns()),
                buildColumnQuestions(mediaPair.getAllColumns()),
                buildODPValue(mediaPair.getAllColumns().stream().filter(columnPair -> !columnPair.isShardKey()).map(ColumnPair::getTargetColumnName).collect(Collectors.toList()))).intern();
        return res;
    }

    private String buildColumnNames(List<ColumnPair> columnPairs) {
        StringBuilder sb = new StringBuilder();
        int size = columnPairs.size();
        for (int i = 0; i < size; i++) {
            sb.append(buildNameForSql(columnPairs.get(i).getTargetColumnName())).append(i + 1 < size ? COMMA : "");
        }
        return sb.toString().intern();
    }

    private String buildColumnQuestions(List<ColumnPair> columnPairs) {
        StringBuilder sb = new StringBuilder();
        int size = columnPairs.size();
        for (int i = 0; i < size; i++) {
            sb.append("?").append(i + 1 < size ? COMMA : "");
        }
        return sb.toString().intern();
    }

    /**
     * getKeyDesc
     * @param targetKeys
     * @return
     */
    private String buildODPValue(List<String> targetKeys) {
        StringBuilder sb = new StringBuilder();
        int size = targetKeys.size();
        for (int i = 0; i < size; i++) {
            String columnName = targetKeys.get(i);
            sb.append(String.format(DUPLICATE_KEY_TEMP, buildNameForSql(columnName), buildNameForSql(columnName)))
                    .append(i + 1 < size ? COMMA : "");
        }
        return sb.toString().intern();
    }

    private String buildSchemaAndTableName(String schemaName, String tableName) {
        return buildNameForSql(schemaName).concat(DOT).concat(buildNameForSql(tableName)).intern();
    }

    private String buildNameForSql(String name) {
        return BACK_QUOTE.concat(name).concat(BACK_QUOTE).intern();
    }

    // singleton
    private SqlBuilder() {}

    public static SqlBuilder getSqlBuilder() {
        return InstanceHolder.instance;
    }

    private static class InstanceHolder{
        private static final SqlBuilder instance = new SqlBuilder();
    }
}
