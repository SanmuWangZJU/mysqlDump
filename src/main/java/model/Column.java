package model;

import lombok.Data;

@Data
public class Column {
    private String columnName;
    private String schemaName;
    private String tableName;

}
