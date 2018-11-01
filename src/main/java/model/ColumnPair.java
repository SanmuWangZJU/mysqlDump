package model;

import lombok.Data;


@Data
public class ColumnPair {

    // columnName是默认源端/目的端一致的，如果不一致，则分别填写source/target
    private String                      columnName;
    private String                      sourceColumnName;
    private String                      targetColumnName;
    // 非向分库分表同步，不要将此属性置为true（不写或者写为false），不支持分片键的更新，也不支持分片键作为主键/唯一索引。shardKey=true的将不会映射到目标表中
    private boolean                     shardKey = false;

    // sourceType, used for get value
    private int                         columnType;
    // targetType is numeric, used for generate load sql( if a column is none numeric, the column should be wrapped by apostrophe')
    private boolean                     TargetNumeric;

}
