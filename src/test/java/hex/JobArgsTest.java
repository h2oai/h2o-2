package hex;

import org.junit.*;

import water.*;
import water.Job.ValidatedJob;
import water.api.RequestServer;
import water.fvec.Frame;
import water.util.Utils.ExpectedExceptionForDebug;

@Ignore
public class JobArgsTest extends HttpTest {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  @Test public void testIndexesVsNames() throws Exception {
    String[] names = new String[] { "5", "2", "1", "8", "4", "100" };
    double[][] items = new double[10][names.length];
    for( int r = 0; r < items.length; r++ )
      for( int c = 0; c < items[r].length; c++ )
        items[r][c] = r;

    Key key = Key.make("test");
    Key dst = Key.make("dest");
    Frame frame = frame(names, items);
    UKV.put(key, frame);
    try {
      RequestServer.registerRequest(new FailTestJob());
      RequestServer.registerRequest(new FailTestJobAsync());
      RequestServer.registerRequest(new ArgsTestJob());

      String args = "" + //
          "destination_key=" + dst + "&" + //
          "source=" + key + "&" + //
          "response=8&" + //
          "cols=2,100";
      Get get;

      get = get("NotRegisteredJob.json?" + args, Res.class);
      Assert.assertEquals(404, get._status);
      waitForJob(dst);

      get = get(FailTestJob.class.getSimpleName() + ".json?" + args, Res.class);
      Assert.assertEquals(500, get._status);
      waitForJob(dst);

      get = get(FailTestJobAsync.class.getSimpleName() + ".json?" + args, Res.class);
      Assert.assertEquals(200, get._status);
      String exception = waitForJob(dst);
      Assert.assertTrue(exception.contains(ExpectedExceptionForDebug.class.getName()));

      get = get(ArgsTestJob.class.getSimpleName() + ".json?" + args, Res.class);
      Assert.assertEquals(200, get._status);
      waitForJob(dst);
    } finally {
      RequestServer.unregisterRequest(new FailTestJob());
      RequestServer.unregisterRequest(new FailTestJobAsync());
      RequestServer.unregisterRequest(new ArgsTestJob());
      UKV.remove(key);
    }
  }

  static class Res {
    String status;
  }

  static class FailTestJob extends ValidatedJob {
    @Override protected Response serve() {
      throw new ExpectedExceptionForDebug();
    }
  }

  static class FailTestJobAsync extends ValidatedJob {
    @Override protected void execImpl() {
      throw new ExpectedExceptionForDebug();
    }
  }

  static class ArgsTestJob extends ValidatedJob {
    @Override protected void execImpl() {
      Assert.assertEquals(source.vecs()[1], _train[0]);
      Assert.assertEquals(source.vecs()[5], _train[1]);
    }
  }
}
