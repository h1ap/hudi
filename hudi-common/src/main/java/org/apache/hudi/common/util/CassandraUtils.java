package org.apache.hudi.common.util;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class CassandraUtils {

  private static final String HOSTNAME = "172.23.26.65";
  private static final Integer PORT = 30081;
  private static final String USERNAME = "cm";
  private static final String PASSWORD = "sjz123457!";
  private static final String KEY_SPACE = "test";
  private static volatile Cluster cluster;

  public static Cluster getCluster() {
    if (cluster == null) {
      synchronized (CassandraUtils.class) {
        if (cluster == null) {
          try {
            // 3 min
            final PoolingOptions poolingOptions =
                new PoolingOptions().setPoolTimeoutMillis(180 * 1000);
            Cluster clr =
                Cluster.builder()
                    .addContactPoint(HOSTNAME)
                    .withPort(PORT)
                    .withPoolingOptions(poolingOptions)
                    // .withCompression(ProtocolOptions.Compression.SNAPPY)
                    .withCredentials(USERNAME, PASSWORD)
                    .build();
            CassandraUtils.cluster = clr;
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        }
      }
    }
    return cluster;
  }

  private CassandraUtils() {

  }

  public static List<String> lists(String path) {

    try (final Cluster clr = getCluster();
        Session instance = clr.connect(KEY_SPACE)) {
      ResultSet rs = instance.execute(String.format("select * from %s", path));
      final List<Row> rows = rs.all();
      rows.stream().forEach(System.out::println);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return null;
  }

  public static boolean batchInsert(List<Integer> data) {
    int thread = 16;
    final ExecutorService executorService = Executors.newFixedThreadPool(thread);

    final Cluster clr = getCluster();
    AtomicInteger incr = new AtomicInteger();

    List<Session> sessions = new ArrayList<>();

    for (int i = 0; i < thread; ++i) {
      sessions.add(clr.connect(KEY_SPACE));
    }

    for (int i = 0; i < thread * 100000; ++i) {
      int finalI = i;
      executorService.execute(
          () -> {
            try {
              Session session = sessions.get(finalI % thread);
              final BatchStatement batchStatement = new BatchStatement();

              String baseSQL =
                  "INSERT INTO test.table_name (id, name, city, age) VALUES (%s, '%s', '%s', %s)";
              for (int j = 0; j < 512; ++j) {
                String insertSQL =
                    String.format(
                        baseSQL,
                        incr.get(),
                        incr.get() + "",
                        incr.get() + "",
                        incr.getAndIncrement());
                batchStatement.add(new SimpleStatement(insertSQL));
              }
              session.execute(batchStatement);
              System.out.printf(
                  "session id : %s, current times： %s\n", finalI % thread, incr.get());
              batchStatement.clear();
            } catch (Exception ex) {
              // pass
            }
          });
    }
    executorService.shutdown();
    return true;
  }

  public static void main(String[] args) {
    CassandraUtils.batchInsert(new ArrayList<>());
    // CassandraUtils.lists("table_name");
  }
}
