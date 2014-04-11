package water;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import water.parser.ParseDataset;
import water.util.*;

public class TestUtil {
  private static int _initial_keycnt = 0;

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

  @AfterClass public static void checkLeakedKeys() {
    Job[] jobs = Job.all();
    for( Job job : jobs ) {
      assert job.state != JobState.RUNNING : ("UNFINISHED JOB: " + job.job_key + " " + job.description + ", end_time = " + job.end_time + ", state=" + job.state );  // No pending job
      DKV.remove(job.job_key);
    }
    DKV.remove(Job.LIST);         // Remove all keys
    DKV.remove(Log.LOG_KEY);
    DKV.write_barrier();
    int leaked_keys = H2O.store_size() - _initial_keycnt;
    if( leaked_keys > 0 ) {
      for( Key k : H2O.localKeySet() ) {
        Value value = DKV.get(k);
        Object o = value.type() != TypeMap.PRIM_B ? value.get() : "byte[]";
        // Ok to leak VectorGroups
        if( o instanceof Vec.VectorGroup )
          leaked_keys--;
        else
          System.err.println("Leaked key: " + k + " = " + o);
      }
    }
    assertTrue("No keys leaked", leaked_keys <= 0);
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

  public static Key[] load_test_folder(String fname) {
    return load_test_folder(find_test_file(fname));
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

  public static Key load_test_file(String fname, String key) {
    return load_test_file(find_test_file(fname), key);
  }

  public static Key load_test_file(String fname) {
    return load_test_file(find_test_file(fname));
  }

  public static Key load_test_file(File file, String keyname) {
    Key key = VAUtils.loadFile(file, keyname);
    if( key == null )
      fail("failed load to " + file.getName());
    return key;
  }

  public static Key load_test_file(File file) {
    Key key = VAUtils.loadFile(file);
    if( key == null )
      fail("failed load to " + file.getName());
    return key;
  }

  public static Key loadAndParseFile(String keyName, String path) {
    Key fkey = load_test_file(path);
    Key okey = Key.make(keyName);
    ParseDataset.parse(okey, new Key[] { fkey });
    return okey;
  }

  public static Key loadAndParseFolder(String keyName, String path) {
    Key[] keys = load_test_folder(path);
    Arrays.sort(keys);
    Key okey = Key.make(keyName);
    ParseDataset.parse(okey, keys);
    return okey;
  }

  public static ValueArray parse_test_key(Key fileKey, Key parsedKey) {
    return VAUtils.parseKey(fileKey, parsedKey);
  }

  // --------
  // Build a ValueArray from a collection of normal arrays.
  // The arrays must be all the same length.
  public static ValueArray va_maker(Key key, Object... arys) {
    new ValueArray(key,0).delete_and_lock(null);
    // Gather basic column info, 1 column per array
    ValueArray.Column cols[] = new ValueArray.Column[arys.length];
    char off = 0;
    int numrows = -1;
    for( int i = 0; i < arys.length; i++ ) {
      ValueArray.Column col = cols[i] = new ValueArray.Column();
      col._name = "C" + Integer.toString(i+1);
      col._off = off;
      col._scale = 1;
      col._min = Double.MAX_VALUE;
      col._max = Double.MIN_VALUE;
      col._mean = 0.0;
      Object ary = arys[i];
      if( ary instanceof byte[] ) {
        col._size = 1;
        col._n = ((byte[]) ary).length;
      } else if( ary instanceof float[] ) {
        col._size = -4;
        col._n = ((float[]) ary).length;
      } else if( ary instanceof double[] ) {
        col._size = -8;
        col._n = ((double[]) ary).length;
      } else if( ary instanceof String[] ) {
        col._size = 2; // Catagorical: assign size==2
        col._n = ((String[]) ary).length;
        col._domain = new String[0];
      } else if( ary instanceof short[] ) {
        // currently using size==2 (shorts) for Enums instead
        throw H2O.unimpl();
      } else {
        throw H2O.unimpl();
      }
      off += Math.abs(col._size);
      if( numrows == -1 )
        numrows = (int) col._n;
      else
        assert numrows == col._n;
    }

    Futures fs = new Futures();
    int rowsize = off;
    ValueArray ary = new ValueArray(key, numrows, rowsize, cols);
    int row = 0;

    for( int chunk = 0; chunk < ary.chunks(); chunk++ ) {
      // Compact data into VA format, and compute min/max/mean
      int rpc = ary.rpc(chunk);
      int limit = row + rpc;
      AutoBuffer ab = new AutoBuffer(rpc * rowsize);

      for( ; row < limit; row++ ) {
        for( int j = 0; j < arys.length; j++ ) {
          ValueArray.Column col = cols[j];
          double d;
          float f;
          byte b;
          switch( col._size ) {
          // @formatter:off
          case  1: ab.put1 (b = ((byte  [])arys[j])[row]);  d = b;  break;
          case -4: ab.put4f(f = ((float [])arys[j])[row]);  d = f;  break;
          case -8: ab.put8d(d = ((double[])arys[j])[row]);          break;
          // @formatter:on
            case 2: // Categoricals or enums
              String s = ((String[]) arys[j])[row];
              String[] dom = col._domain;
              int k = index(dom, s);
              if( k == dom.length ) {
                col._domain = dom = Arrays.copyOf(dom, k + 1);
                dom[k] = s;
              }
              ab.put2((short) k);
              d = k;
              break;
            default:
              throw H2O.unimpl();
          }
          if( d > col._max )
            col._max = d;
          if( d < col._min )
            col._min = d;
          col._mean += d;
        }
      }

      Key ckey = ary.getChunkKey(chunk);
      DKV.put(ckey, new Value(ckey, ab.bufClose()), fs);
    }

    // Sum to mean
    for( ValueArray.Column col : cols )
      col._mean /= col._n;

    // 2nd pass for sigma. Sum of squared errors, then divide by n and sqrt
    for( int i = 0; i < numrows; i++ ) {
      for( int j = 0; j < arys.length; j++ ) {
        ValueArray.Column col = cols[j];
        double d;
        switch( col._size ) {
        // @formatter:off
          case  1: d = ((byte  [])arys[j])[i];  break;
          case  2: d = index(col._domain,((String[])arys[j])[i]);  break;
          case -4: d = ((float [])arys[j])[i];  break;
          case -8: d = ((double[])arys[j])[i];  break;
          default: throw H2O.unimpl();
          // @formatter:on
        }
        col._sigma += (d - col._mean) * (d - col._mean);
      }
    }
    // RSS to sigma
    for( ValueArray.Column col : cols )
      col._sigma = Math.sqrt(col._sigma / (col._n - 1));

    // Write out data & keys
    ary.unlock(null);
    fs.blockForPending();
    return ary;
  }

  static int index(String[] dom, String s) {
    for( int k = 0; k < dom.length; k++ )
      if( dom[k].equals(s) )
        return k;
    return dom.length;
  }

  // Make a M-dimensional data grid, with N points on each dimension running
  // from 0 to N-1. The grid is flattened, so all N^M points are in the same
  // ValueArray. Add a final column which is computed by running an expression
  // over the other columns, typically this final column is the input to GLM
  // which then attempts to recover the expression.
  public abstract static class DataExpr {
    public abstract double expr(byte[] cols);
  }

  @SuppressWarnings("cast") public ValueArray va_maker(Key key, int M, int N, DataExpr expr) {
    if( N <= 0 || N > 127 || M <= 0 )
      throw H2O.unimpl();
    long Q = 1;
    for( int i = 0; i < M; i++ ) {
      Q *= N;
      if( (long) (int) Q != Q )
        throw H2O.unimpl();
    }
    byte[][] x = new byte[M][(int) Q];
    double[] d = new double[(int) Q];

    byte[] bs = new byte[M];
    int q = 0;
    int idx = M - 1;
    d[q++] = expr.expr(bs);
    while( idx >= 0 ) {
      if( ++bs[idx] >= N ) {
        bs[idx--] = 0;
      } else {
        idx = M - 1;
        for( int i = 0; i < M; i++ )
          x[i][q] = bs[i];
        d[q++] = expr.expr(bs);
      }
    }
    Object[] arys = new Object[M + 1];
    for( int i = 0; i < M; i++ )
      arys[i] = x[i];
    arys[M] = d;
    return va_maker(key, arys);
  }

  // Fluid Vectors

  public static Frame parseFromH2OFolder(String path) {
    File file = new File(VM.h2oFolder(), path);
    return FrameUtils.parseFrame(null, file);
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
  public static float []   ar (float  ...a)   { return a; }
  public static double[]   ar (double ...a)   { return a; }
  public static double[][] ar (double[] ...a) { return a; }
  // Expanded array
  public static double[][] ear (double ...a)   {
    double[][] r = new double[a.length][1];
    for (int i=0; i<a.length;i++) r[i][0] = a[i];
    return r;
  }
}
