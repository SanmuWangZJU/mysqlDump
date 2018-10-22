package model;

import lombok.Data;

import java.util.List;

/**
 * columnPairs是需要同步的列的名字，不包含主键(没有主键可以用uniqueKey)
 * keyPairs是主键 OR uniqueKey信息，是必须有的
 */
@Data
public class MediaPair {
    private MetaInfo sourceMeta;
    private MetaInfo targetMeta;
    private List<ColumnPair> columnPairs;
    private List<ColumnPair> keyPairs;
    // build after init
    private List<ColumnPair> allColumns;

}
