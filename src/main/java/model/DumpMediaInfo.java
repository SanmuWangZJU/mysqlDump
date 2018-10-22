package model;

import lombok.Data;

import java.util.List;

@Data
public class DumpMediaInfo {
    private MysqlMediaSource source;
    private MysqlMediaSource target;
    private List<MediaPair> mediaPairs;
    private int batchSize;
    private boolean usingShard = false; // 目标端是否使用分库分表，默认false
    public boolean isUsingBatch() {
        return batchSize > 1;
    }
}
