package model;

import lombok.*;

import java.util.List;

@Getter
@ToString
@EqualsAndHashCode
@Builder
public class RowData {
    private MediaPair mediaPair;
    private List<ColumnData> value;
    private boolean end = false;
}
