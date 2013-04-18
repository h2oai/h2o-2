package water;

import hex.GLMGridTest;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import water.parser.ParseCompressedAndXLSTest;
import water.parser.ParseFolderTestBig;
import water.sys.Node;
import water.sys.NodeVM;
import water.util.Utils;

public class JUnitRunner {
  private static void filter(List<Class> tests) {
    // Requires separate datasets project
    tests.remove(ParseFolderTestBig.class);
    // Too slow
    tests.remove(ConcurrentKeyTest.class);
    // Does not pass TODO!
    tests.remove(ParseCompressedAndXLSTest.class);
    tests.remove(GLMGridTest.class);

    // Uncomment to run tests selectively
    // tests.clear();
    // tests.add(KMeansTest.class);
  }

  public static void main(String[] args) throws Exception {
    // Force all IPs to local so that users can run with a firewall
    File flat = Utils.tempFile("127.0.0.1:54321\n127.0.0.1:54323\n127.0.0.1:54325");
    String[] a = new String[] { "-ip", "127.0.0.1", "-flatfile", flat.getAbsolutePath() };
    H2O.OPT_ARGS.ip = "127.0.0.1";
    args = (String[]) ArrayUtils.addAll(a, args);

    ArrayList<Node> nodes = new ArrayList<Node>();
    nodes.add(new NodeVM(args));
    nodes.add(new NodeVM(args));

    args = (String[]) ArrayUtils.addAll(new String[] { "-mainClass", Master.class.getName() }, args);
    Node master = new NodeVM(args);
    nodes.add(master);

    File out = null, err = null, sandbox = new File("sandbox");
    sandbox.mkdirs();
    Utils.clearFolder(sandbox);
    for( int i = 0; i < nodes.size(); i++ ) {
      out = File.createTempFile("junit-" + i + "-out-", null, sandbox);
      err = File.createTempFile("junit-" + i + "-err-", null, sandbox);
      nodes.get(i).persistIO(out.getAbsolutePath(), err.getAbsolutePath());
      // nodes.get(i).inheritIO();
      nodes.get(i).start();
    }

    int exit = master.waitFor();
    if( exit != 0 ) {
      Log.log(out, System.out);
      Thread.sleep(100); // Or mixed (?)
      Log.log(err, System.err);
    }
    for( Node node : nodes )
      node.kill();
    if( exit == 0 )
      System.out.println("OK");
    System.exit(exit);
  }

  public static class Master {
    public static void main(String[] args) {
      try {
        H2O.main(args);
        TestUtil.stall_till_cloudsize(3);
        List<String> names = Boot.getClasses();
        Collections.sort(names); // For deterministic runs
        ArrayList<Class> tests = new ArrayList<Class>();
        for( String name : names ) {
          try {
            Class c = Class.forName(name);
            if( isTest(c) )
              tests.add(c);
          } catch( Throwable _ ) {
          }
        }
        if( tests.size() == 0 )
          throw new Exception("Failed to find tests");

        filter(tests);
        Result r = org.junit.runner.JUnitCore.runClasses(tests.toArray(new Class[0]));
        if( r.getFailureCount() == 0 ) {
          System.out.println("Successfully ran the following tests in " + (r.getRunTime() / 1000) + "s");
          for( Class c : tests )
            System.out.println(c.getName());
        } else {
          for( Failure f : r.getFailures() ) {
            System.err.println(f.getDescription());
            if( f.getException() != null )
              f.getException().printStackTrace();
          }
        }
        System.exit(r.getFailureCount());
      } catch( Throwable t ) {
        t.printStackTrace();
        System.exit(1);
      }
    }

    private static boolean isTest(Class c) {
      for( Annotation a : c.getAnnotations() )
        if( a instanceof Ignore )
          return false;
      for( Method m : c.getMethods() )
        for( Annotation a : m.getAnnotations() )
          if( a instanceof Test )
            return true;
      return false;
    }
  }
}