package hex.purelogic;

import junit.framework.Assert;
import org.junit.Test;
import water.TestUtil;
import water.api.RequestServer;

public class EmbeddedPseudoCranTest extends TestUtil {
  private static void check (boolean b) {
    if (! b) {
      Assert.assertTrue(b);
    }
  }

  @Test public void test1() {
    // Check basic stuff.
    System.out.println("test1");

    {
      String input = "/R/bin/windows/contrib/3.0/foo/bar";
      String output = RequestServer.maybeTransformRequest(input);
      check(output.equals(input));
    }

    {
      String input = "/R/bin/windows/contrib/2.15/foo/bar";
      String output = RequestServer.maybeTransformRequest(input);
      check(output.equals("/R/bin/windows/contrib/3.0/foo/bar"));
    }

    {
      String input = "/R/bin/windows/contrib/3.1/foo/bar";
      String output = RequestServer.maybeTransformRequest(input);
      check(output.equals("/R/bin/windows/contrib/3.0/foo/bar"));
    }

    {
      String input = "/R/bin/macos/contrib/3.1/foo/bar";
      String output = RequestServer.maybeTransformRequest(input);
      check(output.equals("/R/bin/macos/contrib/3.0/foo/bar"));
    }

    {
      String input = "/R/src/contrib/h2o_2.1.0.99999.tar.gz";
      String output = RequestServer.maybeTransformRequest(input);
      check(output.equals("/R/src/contrib/h2o_2.1.0.99999.tar.gz"));
    }
  }
}
