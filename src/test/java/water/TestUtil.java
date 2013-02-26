package water;

import static org.junit.Assert.*;

import java.io.*;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import water.parser.ParseDataset;

import com.google.common.io.Closeables;

public class TestUtil {
  @BeforeClass
  public static void setupCloud() {
    H2O.main(new String[] {});
  }

  @AfterClass
  public static void checkLeakedKeys() {
    DKV.write_barrier();
    int leaked = 0;
    for( Key k : H2O.keySet() ) {
      if( !k.equals(Job.LIST) && !k.equals(Types.KEY)) {
        System.err.println("Leaked key: " + k);
        leaked++;
      }
    }
    assertEquals("No keys leaked", 0, leaked);
  }

  // Stall test until we see at least X members of the Cloud
  public static void stall_till_cloudsize(int x) {
    long start = System.currentTimeMillis();
    while( System.currentTimeMillis() - start < 10000 ) {
      if( H2O.CLOUD.size() >= x )
        break;
      try {
        Thread.sleep(100);
      } catch( InterruptedException ie ) {
      }
    }
    assertTrue("Cloud size of " + x, H2O.CLOUD.size() >= x);
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
    return file;
  }

  public static Key load_test_file(String fname) {
    return load_test_file(find_test_file(fname));
  }

  public static Key load_test_file(File file, String keyname) {
    Key key = null;
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
      key = ValueArray.readPut(keyname, fis);
    } catch( IOException e ) {
      Closeables.closeQuietly(fis);
    }
    if( key == null )
      fail("failed load to " + file.getName());
    return key;
  }

  public static Key load_test_file(File file) {
    return load_test_file(file, file.getPath());
  }

  public static Key loadAndParseKey(String keyName, String path) {
    Key fkey = load_test_file(path);
    Key okey = Key.make(keyName);
    ParseDataset.parse(okey, DKV.get(fkey));
    UKV.remove(fkey);
    return okey;
  }

  public static ValueArray parse_test_key(Key fileKey, Key parsedKey) {
    ParseDataset.parse(parsedKey, DKV.get(fileKey));
    return ValueArray.value(DKV.get(parsedKey));
  }

  public static ValueArray parse_test_key(Key fileKey) {
    return parse_test_key(fileKey, Key.make());
  }

  public static String replaceExtension(String fname, String newExt) {
    int i = fname.lastIndexOf('.');
    if( i == -1 )
      return fname + "." + newExt;
    return fname.substring(0, i) + "." + newExt;
  }

  public static String getHexKeyFromFile(File f) {
    return replaceExtension(f.getName(), "hex");
  }

  public static String getHexKeyFromRawKey(String str) {
    if( str.startsWith("hdfs://") )
      str = str.substring(7);
    return replaceExtension(str, "hex");
  }

  // --------
  // Build a ValueArray from a collection of normal arrays.
  // The arrays must be all the same length.
  public static ValueArray va_maker(Key key, Object... arys) {
    UKV.remove(key);
    // Gather basic column info, 1 column per array
    ValueArray.Column cols[] = new ValueArray.Column[arys.length];
    char off = 0;
    int numrows = -1;
    for( int i = 0; i < arys.length; i++ ) {
      ValueArray.Column col = cols[i] = new ValueArray.Column();
      col._name = Integer.toString(i);
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
          case -8: ab.put8d(d = ((double[])arys[j])[row]);  d = d;  break;
          // @formatter:on
          case 2: // Catagoricals or enums
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
      DKV.put(ckey, new Value(ckey, ab.bufClose()));
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
      col._sigma = Math.sqrt(col._sigma / col._n);

    // Write out data & keys
    DKV.put(key, ary.value());
    DKV.write_barrier();
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

  public ValueArray va_maker(Key key, int M, int N, DataExpr expr) {
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
}
