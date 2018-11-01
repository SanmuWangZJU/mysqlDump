package model;

import lombok.Data;

import java.util.List;

@Data
public class DumpMediaInfo {
    private MysqlMediaSource source;
    private MysqlMediaSource target;
    private List<MediaPair> mediaPairs;
    private int batchSize = 1;
    private int extractThreadSize = 5;
    private int loadThreadSize = 5;
    private boolean usingShard = false; // 目标端是否使用分库分表，默认false
    public boolean isUsingBatch() {
        return batchSize > 1;
    }
}
