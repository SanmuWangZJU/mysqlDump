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
    private int loadBatchSize = 2000;
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
                AtomicInteger toLoadBatchCount = new AtomicInteger(0);
                AtomicInteger loadedBatchCount = new AtomicInteger(0);
                String loadSql = SQL_BUILDER.getInsertUpdatePSSqlForTarget(mediaPair);
                BlockingQueue<RowData> queue = productMap.get(mediaPair.hashCode());
                boolean isInterrupted = false;
                List<RowData> datas = new ArrayList<>();
                while (!isInterrupted) {
                    // queue 中最后添加stop信号
                    RowData rowData;
                    while ((rowData = queue.poll()) == null);
                    if (rowData.isEnd()) {
                        log.info("load ended");
                        isInterrupted = true;
                    } else {
                        datas.add(rowData);
                    }
                    if (datas.size() >= loadBatchSize||isInterrupted) {
                        toLoadBatchCount.getAndIncrement();
                        List<RowData> rowDatas = datas;
                        datas = new ArrayList<>();
                        log.debug("consuming data: batch {}; size {}", toLoadBatchCount.get(), rowDatas.size());
                        load_executor.execute(() -> {
                            sqlExecutor.executeInsertToTargetWithPrepareStatement(loadSql, mediaPair, rowDatas);
                            log.debug("consume finish: batch {}", toLoadBatchCount.get());
                            loadedBatchCount.getAndIncrement();
                        });
                    }
                }
                while (true) {
                    // 此时toLoadBatchCount应该已经达到最大值
                    if (toLoadBatchCount.get() == loadedBatchCount.get()) {
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
