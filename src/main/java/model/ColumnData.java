package model;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
@Builder(toBuilder = true)
public class ColumnData {
    private String columnName;
    private String columnValue;
    private boolean isKey;
}
