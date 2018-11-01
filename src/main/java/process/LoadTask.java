package process;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Data
public class LoadTask implements Runnable{
    private static final SqlBuilder SQL_BUILDER = SqlBuilder.getSqlBuilder();
    private static ThreadFactory threadFactory;
    private List<MediaPair> mediaPairs;
    private SqlExecutor sqlExecutor;
    private int loadBatchSize = 1;
    private Map<Integer, BlockingQueue<RowData>> productMap;
    private ExecutorService load_executor = Executors.newCachedThreadPool(threadFactory);
    private AtomicBoolean loadFinish;

    static {
        threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("load-worker-%d")
                .setDaemon(true)
                .setUncaughtExceptionHandler((t, e) -> log.error("error from" + t.getName(), e))
                .build();
    }

    public void run() {
        mediaPairs.forEach(mediaPair -> {
            try {
                AtomicInteger batchCount = new AtomicInteger(0);
                String loadSql = SQL_BUILDER.getInsertUpdatePSSqlForTarget(mediaPair);
                BlockingQueue<RowData> queue = productMap.get(mediaPair.hashCode());
                boolean isInterrupted = false;
                List<RowData> datas = new ArrayList<>();
                while (!isInterrupted) {
                    // queue 中最后添加stop信号
                    RowData rowData = queue.poll();
                    assert rowData != null;
                    if (rowData.isEnd()) {
                        isInterrupted = true;
                    } else {
                        datas.add(rowData);
                    }
                    if (datas.size() >= loadBatchSize||isInterrupted) {
                        batchCount.getAndIncrement();
                        List<RowData> rowDatas = datas;
                        log.info("consume data: batch {}; size {}", batchCount.get(), datas.size());
                        datas = new ArrayList<>();
                        load_executor.execute(() -> {
                            sqlExecutor.executeInsertToTargetWithPrepareStatement(loadSql, mediaPair, rowDatas);
                            batchCount.decrementAndGet();
                        });
                    }
                }
                while (true) {
                    if (batchCount.get() == 0) {
                        load_executor.shutdownNow();
                        loadFinish.set(true);
                        break;
                    }
                }

            } catch (Exception e) {
                throw new DumpException("failed to load to target", e)
                        .addContextValue("mediaInfo", mediaPair);
            }
        });

    }

}
