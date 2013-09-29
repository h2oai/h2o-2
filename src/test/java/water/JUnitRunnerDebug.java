package water;

import hex.GLMTest;

import java.util.ArrayList;

import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import water.deploy.NodeCL;
import water.util.Log;
import water.util.Utils;

public class JUnitRunnerDebug {
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, args);
  }

  public static class UserCode {
    public static void userMain(String[] args) {
      String flat = "";
      flat += "127.0.0.1:54321\n";
      flat += "127.0.0.1:54323\n";
      flat += "127.0.0.1:54325\n";
      flat = Utils.writeFile(flat).getAbsolutePath();

      H2O.main(("  -ip 127.0.0.1 -port 54321 -flatfile " + flat).split(" "));
      new NodeCL(("-ip 127.0.0.1 -port 54323 -flatfile " + flat).split(" ")).start();
      new NodeCL(("-ip 127.0.0.1 -port 54325 -flatfile " + flat).split(" ")).start();

      ArrayList<Class> tests = new ArrayList<Class>();

      // Classes to test:
      tests.add(GLMTest.class);

      Result result = org.junit.runner.JUnitCore.runClasses(tests.toArray(new Class[0]));
      if( result.getFailures().size() == 0 )
        System.out.println("Success!");
      else {
        for( Failure f : result.getFailures() ) {
          Log.info(f.getDescription());
          f.getException().printStackTrace();;
        }
      }
    }
  }
}
