package water;

import org.junit.*;

import water.util.Log;

// A simple harness to load all cores on all machines for 30sec.
// It is an error if all cores do not load... and then also the
// test will run for longer than 30sec.  However, if the cores
// are not idle, then the test may also run longer than 30sec.
public class CoreTest extends TestUtil {
  @BeforeClass public static void stall() { stall_till_cloudsize(2); }

  static int RUNTIME=30*1000;
  static int MAPTIME=1000;

  /*@Test*/ public void testCPULoad() {
    int jobs = H2O.NUMCPUS*RUNTIME/MAPTIME;
    // Target all keys remotely: the bug is that the *remote* JVM loses a core
    // to the DRemoteTask.
    H2O cloud = H2O.CLOUD;
    H2ONode target = cloud._memary[0];
    if( target == H2O.SELF ) target = cloud._memary[1];
    Key[] keys = new Key[jobs];
    for( int i=0; i<keys.length; i++ )
      keys[i] = Key.make("CPU"+i,(byte)1,Key.DFJ_INTERNAL_USER,target);
    long start = System.currentTimeMillis();
    new CPULoad().invoke(keys);
    long now=System.currentTimeMillis();
    Log.unwrap(System.err,"Runtime= "+(now-start)+" Jobs="+jobs+" maptime="+MAPTIME);
  }

  public static class CPULoad extends MRTask {
    double _sum;
    @Override public void map( Key key ) {
      long start = System.currentTimeMillis();
      long stop = start+MAPTIME;
      long now;
      while( (now=System.currentTimeMillis()) < stop )
        _sum += Math.sqrt(now);
    }
    @Override public void reduce( DRemoteTask drt ) {
      CPULoad cpu = (CPULoad)drt;
      _sum += cpu._sum;
    }
  }

  @Test @Ignore public void dummy_test() {
    /* this is just a dummy test to avoid JUnit complains about missing test */
  }
}
