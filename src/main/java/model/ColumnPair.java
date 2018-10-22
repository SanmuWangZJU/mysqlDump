package model;

import lombok.Data;


@Data
public class ColumnPair {

    // columnName是默认源端/目的端一致的，如果不一致，则分别填写source/target
    private String                      columnName;
    private String                      sourceColumnName;
    private String                      targetColumnName;
    private boolean                     shardKey = false;

    // sourceType, used for get value
    private int                         columnType;
    // targetType is numeric, used for generate load sql( if a column is none numeric, the column should be wrapped by apostrophe')
    private boolean                     TargetNumeric;

}
