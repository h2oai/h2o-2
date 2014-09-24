package water;

import jsr166y.CountedCompleter;
import jsr166y.ForkJoinPool;
import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
import org.junit.BeforeClass;
import org.junit.Test;
import water.AutoBuffer.AutoBufferException;
import water.H2O.H2OCountedCompleter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

/**
 * Created by tomasnykodym on 9/18/14.
 */
public class RPCTest extends TestUtil {
  @BeforeClass() public static void setup() { stall_till_cloudsize(3); }

  private static class TestReaderSideTask extends DTask<TestReaderSideTask> {

    byte[] _data;

    public TestReaderSideTask() {
      _data = MemoryManager.malloc1(1 << 22);
    }

    @Override
    public void compute2() {
      Arrays.fill(_data, (byte) 123);
      tryComplete();
    }

    @Override
    public void onAck() {
      for (byte b : _data)
        assertEquals(123, b);
    }
  }


//
//  @Test
//  public void testUnreliableTCP () throws Exception {
//    // test unreliable reader
//    // test UDPs
//    Futures fs = new Futures();
//    final ArrayList<ForkJoinTask> ts = new ArrayList<ForkJoinTask>();
//    for (int j = 0; j < H2O.CLOUD.size(); ++j) {
//      final int fj = j;
//      if (H2O.CLOUD._memary[j] == H2O.SELF)
//        continue;
//      for (int k = 0; k < 1000; ++k) {
//        ts.add(new H2OCountedCompleter() {
//          @Override
//          public void compute2() {
//            new RPC<TestReaderSideTask>(H2O.CLOUD._memary[fj], new TestReaderSideTask()).addCompleter(this).call();
//          }
//        });
//      }
//    }
//    Future f = H2O.submitTask(new H2OCountedCompleter() {
//      @Override
//      public void compute2() {
//        ForkJoinTask.invokeAll(ts);
//        tryComplete();
//      }
//    });
//    int i = 0;
//    while(!f.isDone() && !f.isCancelled()){
//      Thread.sleep(10);
//      for(H2ONode n:H2O.CLOUD._memary)
//        n.resetAllConnections();
//    }
//    f.get();
//  }
}
