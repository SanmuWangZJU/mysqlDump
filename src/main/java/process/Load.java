package process;

import exception.DumpException;
import executor.SqlExecutor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import model.MediaPair;
import model.RowData;
import utils.SqlBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@Data
public class Load {
    private List<MediaPair> mediaPairs;
    private SqlExecutor sqlExecutor;
    private Map<Integer, BlockingQueue<RowData>> productMap;
    private static final SqlBuilder SQL_BUILDER = SqlBuilder.getSqlBuilder();
//    private final Executor taskExecutor = Executors.newFixedThreadPool();

    public void load() {
        mediaPairs.forEach(mediaPair -> {
            try {
                String loadSql = SQL_BUILDER.getInsertUpdatePSSqlForTarget(mediaPair);
                BlockingQueue queue = productMap.get(mediaPair.hashCode());
                boolean isInterrupted = false;
                List<RowData> datas = new ArrayList<>();
                while (!isInterrupted) {
                    // queue 中最后添加stop信号
                    RowData rowData = (RowData) queue.poll();
                    assert rowData != null;
                    if (rowData.isEnd()) {
                        isInterrupted = true;
                        continue;
                    }
                    boolean res = datas.add(rowData);
                }
                sqlExecutor.executeInsertToTargetWithPrepareStatement(loadSql, mediaPair, datas);
            } catch (Exception e) {
                throw new DumpException("failed to load to target", e)
                        .addContextValue("mediaInfo", mediaPair);
            }
        });

    }
}
