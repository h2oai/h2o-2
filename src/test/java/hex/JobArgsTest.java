package hex;

import org.junit.*;

import water.Job.ValidatedJob;
import water.*;
import water.api.RequestServer;
import water.fvec.Frame;

public class JobArgsTest extends HttpTest {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(3);
  }

  @Test public void testIndexesVsNames() throws Exception {
    String[] names = new String[] { "5", "2", "1", "8", "4" };
    double[][] items = new double[10][names.length];
    for( int r = 0; r < items.length; r++ )
      for( int c = 0; c < items[r].length; c++ )
        items[r][c] = r;

    Key key = Key.make("test");
    Key dst = Key.make("dest");
    Frame frame = frame(names, items);
    UKV.put(key, frame);
    try {
      RequestServer.registerRequest(new JobArgsTestJob());

      String args = "" + //
          "destination_key=" + dst + "&" + //
          "source=" + key + "&" + //
          "response=2&" + //
          "cols=2,4";
      Get get = get("JobArgsTestJob.json?" + args, Res.class);
      Assert.assertEquals(200, get._status);
      waitForJob(dst);
    } finally {
      RequestServer.unregisterRequest(new JobArgsTestJob());
      UKV.remove(key);
    }
  }

  static class Res {
    String status;
  }

  static class JobArgsTestJob extends ValidatedJob {
    @Override protected void exec() {
      Assert.assertEquals(source.vecs()[2], _train[0]);
      Assert.assertEquals(source.vecs()[4], _train[1]);
    }
  }
}
