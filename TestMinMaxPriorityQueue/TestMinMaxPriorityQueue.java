import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.collect.MinMaxPriorityQueue;

// test MinMaxPriorityQueue
public class TestMinMaxPriorityQueue {
  private final static Map<Long, Long>
    bucketsLastAccessedTime = new ConcurrentHashMap<>();

  //bucket keys in the order they are accessed
  private static final Comparator<Long> cmp = new Comparator<Long>() {
    @Override
    public int compare(Long bucketId1, Long bucketId2) {
      return Long.compare(bucketsLastAccessedTime.get(bucketId1),
                          bucketsLastAccessedTime.get(bucketId2));
    }
  };

  private static final MinMaxPriorityQueue.Builder<Long>
    builder = MinMaxPriorityQueue.orderedBy(cmp);
  private static final MinMaxPriorityQueue<Long>
    bucketHeap = builder.create();

  static void test() throws Exception {
    long keys[] = {1L, 2L, 3L, 4L};
    for ( long key : keys ) {
      bucketsLastAccessedTime.put(key, System.currentTimeMillis());
      bucketHeap.add(key);
    }

    // dump map and queue
    System.out.format("bucketsLastAccessedTime = %s%nbucketHeap = %s%n",
                      bucketsLastAccessedTime, bucketHeap);

    // relax for a bit, then insert same keys but with different timestamps
    Thread.sleep(1000);

    for ( long key : keys ) {
      bucketsLastAccessedTime.put(key, System.currentTimeMillis());
      bucketHeap.add(key);
    }

    // dump map and queue
    System.out.format("Later:%nbucketsLastAccessedTime = %s%nbucketHeap = %s%n",
                      bucketsLastAccessedTime, bucketHeap);
  }  // test

  public static void main(String args[]) throws Exception {
    test();
    System.out.println("Done");
  }  // main
}
