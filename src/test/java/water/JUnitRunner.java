package water;

import hex.*;
import hex.trees.TreeTestWithBalanceAndCrossVal;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import water.deploy.Node;
import water.deploy.NodeVM;
import water.fvec.ParseExceptionTest;
import water.fvec.ParserTest2;
import water.parser.ParseFolderTestBig;
import water.util.Log;
import water.util.Utils;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JUnitRunner {
  // TODO
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Nightly {
  }

  private static void filter(List<Class> tests) {
    // Requires separate datasets project
    tests.remove(ParseFolderTestBig.class);
    // Too slow
    tests.remove(ConcurrentKeyTest.class);
    tests.remove(DeepLearningIrisTest.Long.class);
    tests.remove(DeepLearningProstateTest.Long.class);
    tests.remove(DeepLearningProstateTest.Mid.class);
    tests.remove(GLMRandomTest.Long.class);
    tests.remove(ParserTest2.ParseAllSmalldata.class);
    tests.remove(ParseExceptionTest.class);
    // Skip abstract test
    tests.remove(TreeTestWithBalanceAndCrossVal.class);

    // Pure JUnit test
//    tests.remove(CBSChunkTest.class);
    //tests.remove(GBMDomainTest.class);
  }

  public static void main(String[] args) throws Exception {
    // Can be necessary to run in parallel to other clouds, so find open ports
    int[] ports = new int[3];
    int port = 54321;
    for( int i = 0; i < ports.length; i++ ) {
      for( ;; ) {
        if( isOpen(port) && isOpen(port + 1) ) {
          ports[i] = port;
          port += 2;
          break;
        }
        port++;
      }
    }
    String flat = "";
    for( int i = 0; i < ports.length; i++ )
      flat += "127.0.0.1:" + ports[i] + "\n";
    // Force all IPs to local so that users can run with a firewall
    String[] a = new String[] { "-ip", "127.0.0.1", "-flatfile", Utils.writeFile(flat).getAbsolutePath() };
    H2O.OPT_ARGS.ip = "127.0.0.1";
    args = (String[]) ArrayUtils.addAll(a, args);

    ArrayList<Node> nodes = new ArrayList<Node>();
    for( int i = 1; i < ports.length; i++ )
      nodes.add(new NodeVM(Utils.append(args, "-port", "" + ports[i])));

    args = Utils.append(new String[] { "-mainClass", Master.class.getName() }, args);
    Node master = new NodeVM(Utils.append(args, "-port", "" + ports[0]));
    nodes.add(master);

    File out = null, err = null, sandbox = new File("sandbox");
    sandbox.mkdirs();
    Utils.clearFolder(sandbox);
    for( int i = 0; i < nodes.size(); i++ ) {
      out = File.createTempFile("junit-" + i + "-out-", null, sandbox);
      err = File.createTempFile("junit-" + i + "-err-", null, sandbox);
      nodes.get(i).persistIO(out.getAbsolutePath(), err.getAbsolutePath());
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

  private static boolean isOpen(int port) throws Exception {
    ServerSocket s = null;
    try {
      s = new ServerSocket(port);
      return true;
    } catch( IOException ex ) {
      return false;
    } finally {
      if( s != null )
        s.close();
    }
  }

  static List<Class> all() {
    List<String> names = Boot.getClasses();
    names.remove("water.Boot"); // In case called from Boot loader
    names.remove("water.Weaver");
    Collections.sort(names); // For deterministic runs
    List<Class> tests = new ArrayList<Class>();
    Log._dontDie = true;
    for( String name : names ) {
      try {
        Class c = Class.forName(name);
        if( isTest(c) )
          tests.add(c);
      } catch( Throwable xe ) {
      }
    }
    if( tests.size() == 0 )
      throw new RuntimeException("Failed to find tests");
    filter(tests);
    return tests;
  }

  public static boolean isTest(Class c) {
    for( Annotation a : c.getAnnotations() )
      if( a instanceof Ignore )
        return false;
    for( Method m : c.getMethods() )
      for( Annotation a : m.getAnnotations() )
        if( a instanceof Test )
          return true;
    return false;
  }

  public static class Master {
    public static void main(String[] args) {
      try {
        H2O.main(args);
        TestUtil.stall_till_cloudsize(3);
        List<Class> tests = JUnitRunner.all();
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
  }
}
