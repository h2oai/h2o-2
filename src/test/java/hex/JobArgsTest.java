package hex;

import org.junit.*;

import water.Job.ValidatedJob;
import water.*;
import water.api.RequestServer;
import water.api.RequestServer.API_VERSION;
import water.fvec.Frame;
import water.util.Utils.ExpectedExceptionForDebug;

@Ignore
public class JobArgsTest extends HttpTest {
  @BeforeClass public static void stall() {
    //   stall_till_cloudsize(3);
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
      RequestServer.registerRequest(new FailTestJob());
      RequestServer.registerRequest(new FailTestJobAsync());
      RequestServer.registerRequest(new ArgsTestJob());

      String args = "" + //
          "destination_key=" + dst + "&" + //
          "source=" + key + "&" + //
          "response=2&" + //
          "cols=2,4";
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
      RequestServer.registerRequest(new FailTestJobAsync());
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
    @Override protected void exec() {
      throw new ExpectedExceptionForDebug();
    }
  }

  static class ArgsTestJob extends ValidatedJob {
    @Override protected void exec() {
      Assert.assertEquals(source.vecs()[2], _train[0]);
      Assert.assertEquals(source.vecs()[4], _train[1]);
    }
  }
}
