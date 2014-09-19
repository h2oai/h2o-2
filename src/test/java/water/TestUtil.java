package water;

import static org.junit.Assert.*;
import hex.ConfusionMatrix;
import hex.gbm.DTree.TreeModel;
import hex.glm.GLMModel;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import water.Job.JobState;
import water.deploy.*;
import water.fvec.*;
import water.util.*;

public class TestUtil {
  private static int _initial_keycnt = 0;
  private static Timer _testClassTimer;

  protected static void startCloud(String[] args, int nnodes) {
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes);
  }

  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] {});
    _initial_keycnt = H2O.store_size();
    assert Job.all().length == 0;      // No outstanding jobs
    _testClassTimer = new Timer();
  }

  /** Execute this rule before each test to print test name and test class */
  @Rule public TestRule logRule = new TestRule() {

    @Override public Statement apply(Statement base, Description description) {
      Log.info("###########################################################");
      Log.info("  * Test class name:  " + description.getClassName());
      Log.info("  * Test method name: " + description.getMethodName());
      Log.info("###########################################################");
      return base;
    }
  };

  @Rule public TestRule timerRule = new TestRule() {
    @Override public Statement apply(Statement base, Description description) {
      return new TimerStatement(base, description.getClassName()+"#"+description.getMethodName());
    };
    class TimerStatement extends Statement {
      private final Statement base;
      private final String tname;
      public TimerStatement(Statement base, String tname) { this.base = base; this.tname = tname;}
      @Override public void evaluate() throws Throwable {
        Timer t = new Timer();
        try {
          base.evaluate();
        } finally {
          Log.info("#### TEST "+tname+" EXECUTION TIME: " + t.toString());
        }
      }
    }
  };

  @AfterClass public static void checkLeakedKeys() {
    Log.info("## TEST CLASS EXECUTION TIME (sum over all tests): " + _testClassTimer.toString());
    Job[] jobs = Job.all();
    for( Job job : jobs ) {
      assert job.state != JobState.RUNNING : ("UNFINISHED JOB: " + job.job_key + " " + job.description + ", end_time = " + job.end_time + ", state=" + job.state );  // No pending job
      DKV.remove(job.job_key);
    }
    DKV.remove(Job.LIST);         // Remove all keys
    if (Log.LOG_KEY!=null) DKV.remove(Log.LOG_KEY); // The job key does not need to be created if the test does not print into logs
    DKV.write_barrier();
    int leaked_keys = H2O.store_size() - _initial_keycnt;
    int nvecs = 0, nchunks = 0, nframes = 0, nmodels = 0, nothers = 0;
    if( leaked_keys > 0 ) {
      for( Key k : H2O.localKeySet() ) {
        Value value = DKV.get(k);
        if( value==null ) { leaked_keys--; continue; }
        Object o = value.type() != TypeMap.PRIM_B ? value.get() : "byte[]";
        // Ok to leak VectorGroups
        if( o instanceof Vec.VectorGroup )
          leaked_keys--;
        else {
          try {
            System.err.println("Leaked key: " + k + " = " + o);
          } catch (NullPointerException t) {
            System.err.println("Leaked key: " + k + " = " + o.getClass().getSimpleName() + " with missing data");
          } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
          }

          if (k.isChunkKey()) nchunks++;
          else if (k.isVec()) nvecs++;
          else if (o instanceof Frame) nframes++;
          else if (o instanceof Model) nmodels++;
          else nothers++;
        }
      }
    }
    assertTrue("Key leak! #keys(" + leaked_keys + ") = #vecs("+nvecs+")+#chunks("+nchunks+")+#frames("+nframes+")+#nmodels("+nmodels+")+#others("+nothers+")", leaked_keys <= 0);

    _initial_keycnt = H2O.store_size();
  }

  // Stall test until we see at least X members of the Cloud
  public static void stall_till_cloudsize(int x) {
    stall_till_cloudsize(x, 10000);
  }

  public static void stall_till_cloudsize(int x, long ms) {
    H2O.waitForCloudSize(x, ms);
    UKV.put(Job.LIST, new Job.List()); // Jobs.LIST must be part of initial keys
  }

  public static File find_test_file(String fname) {
    // When run from eclipse, the working directory is different.
    // Try pointing at another likely place
    File file = new File(fname);
    if( !file.exists() )
      file = new File("target/" + fname);
    if( !file.exists() )
      file = new File("../" + fname);
    if( !file.exists() )
      file = new File("../target/" + fname);
    if( !file.exists() )
      file = null;
    return file;
  }


  // Old VA-style wrappers for tests
  public static Key load_test_file(String fname) {
    return load_test_file(find_test_file(fname));
  }

  public static Key load_test_file(File file) { return NFSFileVec.make(file); }

  public static Key loadAndParseFile(String keyName, String path) {
    Key okey = Key.make(keyName);
    ParseDataset2.parse(okey, new Key[]{load_test_file(path)});
    return okey;
  }

  public static Key[] load_test_folder(File folder) {
    assert folder.isDirectory();
    ArrayList<Key> keys = new ArrayList<Key>();
    for( File f : folder.listFiles() ) {
      if( f.isFile() )
        keys.add(load_test_file(f));
    }
    Key[] res = new Key[keys.size()];
    keys.toArray(res);
    return res;
  }

  public static Key loadAndParseFolder(String keyname, String path) {
    Key[] keys = load_test_folder(new File(path));
    Arrays.sort(keys);
    Key okey = Key.make(keyname);
    ParseDataset2.parse(okey, keys);
    return okey;
  }

  // Fluid Vectors

  public static Frame parseFromH2OFolder(String path) {
    File file = new File(VM.h2oFolder(), path);
    return FrameUtils.parseFrame(null, file);
  }

  public static Frame parseFrame(String path) {
    return FrameUtils.parseFrame(null, find_test_file(path));
  }

  public static Frame parseFrame(File file) {
    return FrameUtils.parseFrame(null, file);
  }

  public static Frame parseFrame(Key okey, String path) {
    return FrameUtils.parseFrame(okey, find_test_file(path));
  }

  public static Frame parseFrame(Key okey, File f) {
    return FrameUtils.parseFrame(okey, f);
  }

  public static Vec vec(int...rows) { return vec(null, null, rows); }
  public static Vec vec(String[] domain, int ...rows) { return vec(null, domain, rows); }

  public static Vec vec(Key k, String[] domain, int ...rows) {
    k = (k==null) ? new Vec.VectorGroup().addVec() : k;
    Futures fs = new Futures();
    AppendableVec avec = new AppendableVec(k);
    NewChunk chunk = new NewChunk(avec, 0);
    for( int r = 0; r < rows.length; r++ )
      chunk.addNum(rows[r]);
    chunk.close(0, fs);
    Vec vec = avec.close(fs);
    fs.blockForPending();
    vec._domain = domain;
    return vec;
  }

  public static Frame frame(String name, Vec vec)       { return FrameUtils.frame(name, vec); }
  public static Frame frame(String[] names, Vec[] vecs) { return FrameUtils.frame(names, vecs); }
  public static Frame frame(String[] names, double[]... rows) { return FrameUtils.frame(names, rows); }

  public static void dumpKeys(String msg) {
    System.err.println("-->> Store dump <<--");
    System.err.println("    " + msg);
    System.err.println(" Keys: " + H2O.store_size());
    for ( Key k : H2O.localKeySet()) System.err.println(" * " + k);
    System.err.println("----------------------");
  }

  public static String[]   ar (String ...a)   { return a; }
  public static long  []   ar (long   ...a)   { return a; }
  public static long[][]   ar (long[] ...a)   { return a; }
  public static int   []   ari(int    ...a)   { return a; }
  public static int [][]   ar (int[]  ...a)   { return a; }
  public static float []   arf(float  ...a)   { return a; }
  public static double[]   ard(double ...a)   { return a; }
  public static double[][] ard(double[] ...a) { return a; }
  // Expanded array
  public static double[][] ear (double ...a)   {
    double[][] r = new double[a.length][1];
    for (int i=0; i<a.length;i++) r[i][0] = a[i];
    return r;
  }

  public static void assertCM(long[][] expectedCM, long[][] givenCM) {
    Assert.assertEquals("Confusion matrix dimension does not match", expectedCM.length, givenCM.length);
    String m = "Expected: " + Arrays.deepToString(expectedCM) + ", but was: " + Arrays.deepToString(givenCM);
    for (int i=0; i<expectedCM.length; i++) Assert.assertArrayEquals(m, expectedCM[i], givenCM[i]);
  }

  public static void assertCMEquals(String msg, ConfusionMatrix a, ConfusionMatrix b) {
    Assert.assertEquals(msg + " - Confusion matrix should be of the same size", a._arr.length, b._arr.length);
    for (int i=0; i< a._arr.length; i++) {
      Assert.assertArrayEquals(msg, a._arr[i], b._arr[i]);
    }
  }

  public static void assertModelEquals(Model a, Model b) {
    assertArrayEquals("Model names has to equal!", a._names, b._names);
    assertEquals("Model has to contain same number of domains!", a._domains.length, b._domains.length);
    for (int i=0; i<a._domains.length; i++) {
      assertArrayEquals("Model input column "+i+" has to contain same domain names!", a._domains[i], b._domains[i]);
    }
  }
  public static void assertTreeModelEquals(TreeModel a, TreeModel b) {
    assertModelEquals(a,b);
    assertEquals("Number of demanded trees should be same!", a.N, b.N);
    assertEquals("Number of produced trees should be same!", a.ntrees(), b.ntrees());
    assertArrayEquals("All error fields should be same (requiring models build without skipping scoring)!", a.errs, b.errs, 0.00000001);
    assertEquals("Models shoudl be of the same type!", a.isClassifier(), b.isClassifier());
    if (a.isClassifier()) {
      assertEquals("The models should contain the same number of CMs", a.cms.length, b.cms.length);
      for (int i=0; i<a.cms.length; i++) {
        assertCMEquals(i+"-th CM should be same (requiring models build without skipping scoring)!", a.cms[i], b.cms[i]);
      }
    }
  }
  public static void assertModelBinaryEquals(Model a, Model b) {
    assertArrayEquals("The serialized models are not binary same!", a.write(new AutoBuffer()).buf(), b.write(new AutoBuffer()).buf());
  }

  public static void sleep(int msec) {
    try { Thread.sleep(msec); } catch (InterruptedException e) {}
  }
}
