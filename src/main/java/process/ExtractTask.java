package process;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import exception.DumpException;
import executor.SqlExecutor;
import lombok.Data;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import model.ColumnData;
import model.ColumnPair;
import model.MediaPair;
import model.RowData;
import utils.SqlBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Data
public class ExtractTask implements Runnable{
    private static ThreadFactory threadFactory;
    private static SqlBuilder SQL_BUILDER = SqlBuilder.getSqlBuilder();
    private List<MediaPair> mediaPairs;
    private SqlExecutor sqlExecutor;
    private int extractBatchSize = 1;
    private Map<Integer, BlockingQueue<RowData>> productMap; // init at launcher
    private boolean needOrder = false;
    private int extractThreadSize = 5;
    private AtomicBoolean extractFinish;
    private ExecutorService extract_executor = Executors.newFixedThreadPool(extractThreadSize, threadFactory);

    static {
        threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("extract-worker-%d")
                .setDaemon(true)
                .setUncaughtExceptionHandler((t, e) -> log.error("error from" + t.getName(), e))
                .build();
    }

    public void run() {
        //及时关闭线程池
        CountDownLatch latch = new CountDownLatch(mediaPairs.size());
        Thread demandThread = new Thread(()->{
            try {
                latch.await();
                extractFinish.set(true);
                extract_executor.shutdownNow();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        demandThread.setDaemon(true);
        demandThread.start();

        mediaPairs.parallelStream().forEach(mediaPair -> {
            CyclicBarrier cyclicBarrier = new CyclicBarrier(extractThreadSize, () -> productMap.get(mediaPair.hashCode()).offer(RowData.builder().end(true).build()));
            BlockingQueue<List<String>> sqlQueue = new ArrayBlockingQueue<>(extractThreadSize);
            AtomicInteger startIndex = new AtomicInteger(0);
            AtomicBoolean canGetMore = new AtomicBoolean(true);

            while (canGetMore.get()) {

                int start = startIndex.getAndAdd(extractBatchSize);
                int batchSize = extractBatchSize;
                List<String> selectSqls = buildExtractSql(mediaPair, start, batchSize);
                sqlQueue.offer(selectSqls);

                ExtractWorker worker = new ExtractWorker();
                worker.setCanGetMore(canGetMore);
                worker.setQueue(productMap.get(mediaPair.hashCode()));
                worker.setSqlQueue(sqlQueue);
                worker.setMediaPair(mediaPair);
                worker.setCyclicBarrier(cyclicBarrier);

                extract_executor.execute(worker);
            }
            latch.countDown();
        });
    }

    private List<String> buildExtractSql(MediaPair mediaPair, Integer start, Integer batchSize) {
        if (start != null && batchSize != null) {
            return SQL_BUILDER.getSelectSQLWithLimit(mediaPair, start, batchSize);
        } else {
            return SQL_BUILDER.getSelectSQL(mediaPair);
        }
    }

    private List<String> buildExtractSql(MediaPair mediaPair){
        return buildExtractSql(mediaPair, null, null);
    }

    class ExtractWorker implements Runnable {

        private List<String> selectSqls;
        @Setter
        private BlockingQueue<RowData> queue;
        @Setter
        private AtomicBoolean canGetMore;
        @Setter
        private MediaPair mediaPair;
        @Setter
        private CyclicBarrier cyclicBarrier;
        @Setter
        private BlockingQueue<List<String>> sqlQueue;

        @Override
        public void run() {
            if (canGetMore.get()) {
                AtomicBoolean hasMore = new AtomicBoolean(true);
                selectSqls = sqlQueue.poll();
                if (selectSqls != null) {
                    selectSqls.parallelStream().forEach(selectSql -> {
                        try {
                            ResultSet rs = null;
                            if (canGetMore.get()) {
                                rs = sqlExecutor.executeSelectFromSource(selectSql);
                            }
                            if (rs == null || !rs.isBeforeFirst()) {
                                hasMore.set(false);
                            }
                            while (rs != null && rs.next()) {
                                List<ColumnData> columnDatas = new ArrayList<>();
                                for (ColumnPair columnPair : mediaPair.getAllColumns()) {
                                    // 使用rs.getDouble()等columnType本身的方法时，如果表中值为null，则会返回0；若向表示为null，应该用getString(),取到值后另行判断
                                    String columnValue = rs.getString(columnPair.getSourceColumnName());
                                    columnDatas.add(ColumnData.builder()
                                            .columnName(columnPair.getTargetColumnName())
                                            .columnValue(columnValue)
                                            .isKey(mediaPair.getKeyPairs().contains(columnPair))
                                            .build());
                                }
                                RowData data = RowData.builder().mediaPair(mediaPair).value(columnDatas).build();
                                queue.offer(data);
                            }
                            // 其它worker表示已经没有新数据可以select到了，直接结束
                            if (!canGetMore.get()) {
                                hasMore.set(false);
                            }
                        } catch (SQLException e) {
                            e.printStackTrace();
                            throw new DumpException("error occur while extract from source", e)
                                    .addContextValue("source", mediaPair.getSourceMeta())
                                    .addContextValue("extract sql", selectSql);
                        } finally {
                            // 如果所有SQL select出来的resultSet是空，则说明没有更多数据可以select到，则结束整体extract流程
                            if (!hasMore.get()) {
                                try {
                                    // 待所有线程结束
                                    cyclicBarrier.await();
                                    canGetMore.set(false);
                                } catch (InterruptedException | BrokenBarrierException e) {
                                    e.printStackTrace();
                                    throw new DumpException("error occur while waiting for cyclicBarrier");
                                }
                            }
                        }
                    });
                }
            }
        }
    }

    void shutDown() {
        extract_executor.shutdownNow();
    }

}
