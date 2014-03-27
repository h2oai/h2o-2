package water.util;

import hex.rng.H2ORandomRNG;
import hex.rng.H2ORandomRNG.RNGKind;
import hex.rng.H2ORandomRNG.RNGType;
import hex.rng.MersenneTwisterRNG;
import hex.rng.XorShiftRNG;
import org.junit.Assert;
import org.junit.Test;
import sun.misc.Unsafe;
import water.*;
import water.api.DocGen;
import water.api.DocGen.FieldDoc;
import water.nbhm.UtilUnsafe;
import water.parser.ParseDataset;
import water.parser.ParseDataset.Compression;

import java.io.*;
import java.net.Socket;
import java.security.SecureRandom;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class Utils {
  /** Returns the index of the largest value in the array.
   * In case of a tie, an the index is selected randomly.
   */
  public static int maxIndex(int[] from, Random rand) {
    assert rand != null;
    int result = 0;
    int maxCount = 0; // count of maximal element for a 1 item reservoir sample
    for( int i = 1; i < from.length; ++i ) {
      if( from[i] > from[result] ) {
        result = i;
        maxCount = 1;
      } else if( from[i] == from[result] ) {
        if( rand.nextInt(++maxCount) == 0 ) result = i;
      }
    }
    return result;
  }

  public static int maxIndex(int[] from) {
    int result = 0;
    for (int i = 1; i<from.length; ++i)
      if (from[i]>from[result]) result = i;
    return result;
  }
  public static int maxIndex(float[] from) {
    int result = 0;
    for (int i = 1; i<from.length; ++i)
      if (from[i]>from[result]) result = i;
    return result;
  }
  public static int minIndex(int[] from) {
    int result = 0;
    for (int i = 1; i<from.length; ++i)
      if (from[i]<from[result]) result = i;
    return result;
  }
  public static int minIndex(float[] from) {
    int result = 0;
    for (int i = 1; i<from.length; ++i)
      if (from[i]<from[result]) result = i;
    return result;
  }
  public static double maxValue(double[] from) {
    double result = from[0];
    for (int i = 1; i<from.length; ++i)
      if (from[i]>result) result = from[i];
    return result;
  }
  public static float maxValue(float[] from) {
    float result = from[0];
    for (int i = 1; i<from.length; ++i)
      if (from[i]>result) result = from[i];
    return result;
  }
  public static float minValue(float[] from) {
    float result = from[0];
    for (int i = 1; i<from.length; ++i)
      if (from[i]<result) result = from[i];
    return result;
  }
  public static long maxValue(long[] from) {
    long result = from[0];
    for (int i = 1; i<from.length; ++i)
      if (from[i]>result) result = from[i];
    return result;
  }
  public static long minValue(long[] from) {
    long result = from[0];
    for (int i = 1; i<from.length; ++i)
      if (from[i]<result) result = from[i];
    return result;
  }

  /**
   * Compare two numbers to see if they are within one ulp of the smaller decade.
   * Order of the arguments does not matter.
   *
   * @param a First number
   * @param b Second number
   * @return true if a and b are essentially equal, false otherwise.
   */
  public static boolean equalsWithinOneSmallUlp(float a, float b) {
    float ulp_a = Math.ulp(a);
    float ulp_b = Math.ulp(b);
    float small_ulp = Math.min(ulp_a, ulp_b);
    float absdiff_a_b = Math.abs(a - b); // subtraction order does not matter, due to IEEE 754 spec
    return absdiff_a_b <= small_ulp;
  }

  public static boolean equalsWithinOneSmallUlp(double a, double b) {
    double ulp_a = Math.ulp(a);
    double ulp_b = Math.ulp(b);
    double small_ulp = Math.min(ulp_a, ulp_b);
    double absdiff_a_b = Math.abs(a - b); // subtraction order does not matter, due to IEEE 754 spec
    return absdiff_a_b <= small_ulp;
  }

  public static double lnF(double what) {
    return (what < 1e-06) ? 0 : what * Math.log(what);
  }

  public static String p2d(double d) { return !Double.isNaN(d) ? new DecimalFormat ("0.##"   ).format(d) : "nan"; }
  public static String p5d(double d) { return !Double.isNaN(d) ? new DecimalFormat ("0.#####").format(d) : "nan"; }

  public static int set4( byte[] buf, int off, int x ) {
    for( int i=0; i<4; i++ ) buf[i+off] = (byte)(x>>(i<<3));
    return 4;
  }
  public static int get4( byte[] buf, int off ) {
    int sum=0;
    for( int i=0; i<4; i++ ) sum |= (0xff&buf[off+i])<<(i<<3);
    return sum;
  }

  public static int set8d( byte[] buf, int off, double d ) {
    long x = Double.doubleToLongBits(d);
    for( int i=0; i<8; i++ ) buf[i+off] = (byte)(x>>(i<<3));
    return 8;
  }
  public static double get8d( byte[] buf, int off ) {
    long sum=0;
    for( int i=0; i<8; i++ ) sum |= ((long)(0xff&buf[off+i]))<<(i<<3);
    return Double.longBitsToDouble(sum);
  }

  public static long sum(final long[] from) {
    long result = 0;
    for (long d: from) result += d;
    return result;
  }
  public static int sum(final int[] from) {
    int result = 0;
    for (int d: from) result += d;
    return result;
  }
  public static float sum(final float[] from) {
    float result = 0;
    for (float d: from) result += d;
    return result;
  }
  public static double sum(final double[] from) {
    double result = 0;
    for (double d: from) result += d;
    return result;
  }

  public static String sampleToString(int[] val, int max) {
    if (val == null || val.length < max) return Arrays.toString(val);

    StringBuilder b = new StringBuilder();
    b.append('[');
    max -= 10;
    int valMax = val.length -1;
    for (int i = 0; ; i++) {
        b.append(val[i]);
        if (i == max) {
          b.append(", ...");
          i = val.length - 10;
        }
        if ( i == valMax) {
          return b.append(']').toString();
        }
        b.append(", ");
    }
  }

  /* Always returns a deterministic java.util.Random RNG.
   *
   * The determinism is important for re-playing sampling.
   */
  public static Random getDeterRNG(long seed) { return new H2ORandomRNG(seed); }

  public static void setUsedRNGKind(final RNGKind kind) {
    switch (kind) {
    case DETERMINISTIC:
      setUsedRNGType(RNGType.MersenneTwisterRNG);
      break;
    case NON_DETERMINISTIC:
      setUsedRNGType(RNGType.SecureRNG);
      break;
    }
  }

  /* Returns the configured random generator */
  public synchronized static Random getRNG(long... seed) {
    assert _rngType != null : "Random generator type has to be configured";
    switch (_rngType) {
    case JavaRNG:
      assert seed.length >= 1;
      return new H2ORandomRNG(seed[0]);
    case MersenneTwisterRNG:
      // do not copy the seeds - use them, and initialize the first two ints by seeds based given argument
      // the call is locked, and also MersenneTwisterRNG will just copy the seeds into its datastructures
      assert seed.length == 1;
      int[] seeds    = MersenneTwisterRNG.SEEDS;
      int[] inSeeds = unpackInts(seed);
      seeds[0] = inSeeds[0];
      seeds[1] = inSeeds[1];
      return new MersenneTwisterRNG(seeds);
    case XorShiftRNG:
      assert seed.length >= 1;
      return new XorShiftRNG(seed[0]);
    case SecureRNG:
      return new SecureRandom();
    }

    throw new IllegalArgumentException("Unknown random generator type: " + _rngType);
  }

  private static RNGType _rngType = RNGType.MersenneTwisterRNG;

  public static void setUsedRNGType(RNGType rngType) {
    Utils._rngType = rngType;
  }

  public static RNGType getUsedRNGType() {
    return Utils._rngType;
  }

  public static RNGKind getUsedRNGKind() {
    return Utils._rngType.kind();
  }

  /*
   * Compute entropy value for an array of bytes.
   *
   * The returned number represents entropy per bit!
   * For good long number seed (8bytes seed) it should be in range <2.75,3> (higher is better)
   *
   * For large set of bytes (>100) it should be almost 8 (means almost 8 random bits per byte).
   */
  public static float entropy(byte[] f) {
    int counts[] = new int[256];
    float entropy = 0;
    float total = f.length;

    for (byte b : f) counts[b+128]++;
    for (int c : counts) {
      if (c == 0) continue;
      float p = c / total;

      /* Compute entropy per bit in byte.
       *
       * To compute entropy per byte compute log with base 256 = log(p)/log(256).
       */
      entropy -= p * Math.log(p)/Math.log(2);
    }

    return entropy;
  }

  public static int[] unpackInts(long... longs) {
    int len      = 2*longs.length;
    int result[] = new int[len];
    int i = 0;
    for (long l : longs) {
      result[i++] = (int) (l & 0xffffffffL);
      result[i++] = (int) (l>>32);
    }
    return result;
  }

  public static void shuffleArray(long[] a, long seed) {
    int n = a.length;
    Random random = getDeterRNG(seed);
    random.nextInt();
    for (int i = 0; i < n; i++) {
      int change = i + random.nextInt(n - i);
      swap(a, i, change);
    }
  }

  public static int[] shuffleArray(int[] a, int n, int result[], long seed, int startIndex) {
    if (n<=0) return result;
    Random random = getDeterRNG(seed);
    result[0] = a[startIndex];
    for (int i = 1; i < n; i++) {
      int j = random.nextInt(i+1);
      if (j!=i) result[i] = a[startIndex+j];
      result[j] = a[startIndex+i];
    }
    return result;
  }

  private static void swap(long[] a, int i, int change) {
    long helper = a[i];
    a[i] = a[change];
    a[change] = helper;
  }

  private static void swap(int[] a, int i, int change) {
    int helper = a[i];
    a[i] = a[change];
    a[change] = helper;
  }

  public static void close(Closeable...closeable) {
    for(Closeable c : closeable)
      try { if( c != null ) c.close(); } catch( IOException _ ) { }
  }

  public static void close(Socket s) {
    try { if( s != null ) s.close(); } catch( IOException _ ) { }
  }

  public static String readConsole() {
    BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
    try {
      return console.readLine();
    } catch( IOException e ) {
      throw  Log.errRTExcept(e);
    }
  }

  public static File writeFile(String content) {
    try {
      return writeFile(File.createTempFile("h2o", null), content);
    } catch( IOException e ) {
      throw Log.errRTExcept(e);
    }
  }

  public static File writeFile(File file, String content) {
    FileWriter w = null;
    try {
      w = new FileWriter(file);
      w.write(content);
    } catch(IOException e) {
      Log.errRTExcept(e);
    } finally {
      close(w);
    }
    return file;
  }

  public static void writeFileAndClose(File file, InputStream in) {
    OutputStream out = null;
    try {
      out = new FileOutputStream(file);
      byte[] buffer = new byte[1024];
      int len = in.read(buffer);
      while (len > 0) {
        out.write(buffer, 0, len);
        len = in.read(buffer);
      }
    } catch(IOException e) {
      throw Log.errRTExcept(e);
    } finally {
      close(in, out);
    }
  }

  public static String readFile(File file) {
    FileReader r = null;
    try {
      r = new FileReader(file);
      char[] data = new char[(int) file.length()];
      r.read(data);
      return new String(data);
    } catch(IOException e) {
      throw Log.errRTExcept(e);
    } finally {
      close(r);
    }
  }

  public static void readFile(File file, OutputStream out) {
    BufferedInputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(file));
      byte[] buffer = new byte[1024];
      while( true ) {
        int count = in.read(buffer);
        if( count == -1 )
          break;
        out.write(buffer, 0, count);
      }
    } catch(IOException e) {
      throw Log.errRTExcept(e);
    } finally {
      close(in);
    }
  }

  public static String join(char sep, Object[] array) {
    return join(sep, Arrays.asList(array));
  }

  public static String join(char sep, Iterable it) {
    String s = "";
    for( Object o : it )
      s += (s.length() == 0 ? "" : sep) + o.toString();
    return s;
  }

  public static byte[] or(byte[] a, byte[] b) {
    for(int i = 0; i < a.length; i++ ) a[i] |= b[i];
    return a;
  }
  public static int[] or(int[] a, int[] b) {
    for(int i = 0; i < a.length; i++ ) a[i] |= b[i];
    return a;
  }
  public static byte[] add(byte[] a, byte[] b) {
    for(int i = 0; i < a.length; i++ ) a[i] += b[i];
    return a;
  }
  public static int[] add(int[] a, int[] b) {
    for(int i = 0; i < a.length; i++ ) a[i] += b[i];
    return a;
  }
  public static int[][] add(int[][] a, int[][] b) {
    for(int i = 0; i < a.length; i++ ) add(a[i],b[i]);
    return a;
  }
  public static long[] add(long[] a, long[] b) {
    if( b==null ) return a;
    for(int i = 0; i < a.length; i++ ) a[i] += b[i];
    return a;
  }
  public static long[][] add(long[][] a, long[][] b) {
    for(int i = 0; i < a.length; i++ ) add(a[i],b[i]);
    return a;
  }
  public static float[] add(float[] a, float[] b) {
    if( b==null ) return a;
    for(int i = 0; i < a.length; i++ ) a[i] += b[i];
    return a;
  }
  public static float[][] add(float[][] a, float[][] b) {
    for(int i = 0; i < a.length; i++ ) add(a[i],b[i]);
    return a;
  }
  public static double[] add(double[] a, double[] b) {
    if( a==null ) return b;
    for(int i = 0; i < a.length; i++ ) a[i] += b[i];
    return a;
  }
  public static double[][] add(double[][] a, double[][] b) {
    for(int i = 0; i < a.length; i++ ) a[i] = add(a[i],b[i]);
    return a;
  }

  public static double[][] append(double[][] a, double[][] b) {
    double[][] res = new double[a.length + b.length][];
    System.arraycopy(a, 0, res, 0, a.length);
    System.arraycopy(b, 0, res, a.length, b.length);
    return res;
  }

  public static int[] append(int[] a, int[] b) {
    int[] res = new int[a.length + b.length];
    System.arraycopy(a, 0, res, 0, a.length);
    System.arraycopy(b, 0, res, a.length, b.length);
    return res;
  }

  public static String[] append(String[] a, String[] b) {
    String[] res = new String[a.length + b.length];
    System.arraycopy(a, 0, res, 0, a.length);
    System.arraycopy(b, 0, res, a.length, b.length);
    return res;
  }

  public static double[] append(double[] a, double e) {
    a = Arrays.copyOf(a,a.length+1);
    a[a.length-1] = e;
    return a;
  }

  public static long[][][] append(long[][][] a, long[][] e) {
    a = Arrays.copyOf(a,a.length+1);
    a[a.length-1] = e;
    return a;
  }

  public static <T> T[] append(T[] a, T... b) {
    if( a==null ) return b;
    T[] tmp = Arrays.copyOf(a,a.length+b.length);
    System.arraycopy(b,0,tmp,a.length,b.length);
    return tmp;
  }

  public static <T> T[] remove(T[] a, int i) {
    T[] tmp = Arrays.copyOf(a,a.length-1);
    System.arraycopy(a,i+1,tmp,i,tmp.length-i);
    return tmp;
  }
  public static int[] remove(int[] a, int i) {
    int[] tmp = Arrays.copyOf(a,a.length-1);
    System.arraycopy(a,i+1,tmp,i,tmp.length-i);
    return tmp;
  }

  public static <T> T[] subarray(T[] a, int off, int len) {
    return Arrays.copyOfRange(a,off,off+len);
  }

  public static void clearFolder(String folder) {
    clearFolder(new File(folder));
  }

  public static void clearFolder(File folder) {
    if (folder.exists()) {
      for (File child : folder.listFiles()) {
        if (child.isDirectory())
          clearFolder(child);

        if (!child.delete())
          throw new RuntimeException("Cannot delete " + child);
      }
    }
  }

  /**
   * Returns the system temporary folder, e.g. /tmp
   */
  public static File tmp() {
    try {
      return File.createTempFile("h2o", null).getParentFile();
    } catch( IOException e ) {
      throw new RuntimeException(e);
    }
  }

  public static ValueArray loadAndParseKey(String path) {
    return loadAndParseKey(Key.make(), path);
  }

  public static ValueArray loadAndParseKey(Key okey, String path) {
    FileIntegrityChecker c = FileIntegrityChecker.check(new File(path),false);
    Key k = c.syncDirectory(null,null,null,null);
    ParseDataset.forkParseDataset(okey, new Key[] { k }, null).get();
    UKV.remove(k);
    ValueArray res = DKV.get(okey).get();
    return res;
  }

  public static byte [] getFirstUnzipedBytes(Key k){
    return getFirstUnzipedBytes(DKV.get(k));
  }
  public static byte [] getFirstUnzipedBytes(Value v){
    byte [] bits = v.getFirstBytes();
    try{
      return unzipBytes(bits, guessCompressionMethod(bits));
    } catch(Exception e){return null;}
  }

  public static Compression guessCompressionMethod(byte [] bits){
    AutoBuffer ab = new AutoBuffer(bits);
    // Look for ZIP magic
    if( bits.length > ZipFile.LOCHDR && ab.get4(0) == ZipFile.LOCSIG )
      return Compression.ZIP;
    if( bits.length > 2 && ab.get2(0) == GZIPInputStream.GZIP_MAGIC )
      return Compression.GZIP;
    return Compression.NONE;
  }

  public static byte [] unzipBytes(byte [] bs, Compression cmp) {
    InputStream is = null;
    int off = 0;
    try {
      switch(cmp) {
      case NONE: // No compression
        return bs;
      case ZIP: {
        ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(bs));
        ZipEntry ze = zis.getNextEntry(); // Get the *FIRST* entry
        // There is at least one entry in zip file and it is not a directory.
        if( ze != null && !ze.isDirectory() ) {
          is = zis;
          break;
        }
        zis.close();
        return bs; // Don't crash, ignore file if cannot unzip
      }
      case GZIP:
        is = new GZIPInputStream(new ByteArrayInputStream(bs));
        break;
      default:
        assert false:"cmp = " + cmp;
      }
      // If reading from a compressed stream, estimate we can read 2x uncompressed
      assert( is != null ):"is is NULL, cmp = " + cmp;
      bs = new byte[bs.length * 2];
      // Now read from the (possibly compressed) stream
      while( off < bs.length ) {
        int len = is.read(bs, off, bs.length - off);
        if( len < 0 )
          break;
        off += len;
        if( off == bs.length ) { // Dataset is uncompressing alot! Need more space...
          if( bs.length >= ValueArray.CHUNK_SZ )
            break; // Already got enough
          bs = Arrays.copyOf(bs, bs.length * 2);
        }
      }
    } catch( IOException ioe ) { // Stop at any io error
      Log.err(ioe);
    } finally {
      Utils.close(is);
    }
    return bs;
  }

  /**
   * Simple wrapper around ArrayList with support for H2O serialization
   * @author tomasnykodym
   * @param <T>
   */
  public static class IcedArrayList<T extends Iced> extends ArrayList<T> implements Freezable {
    @Override public AutoBuffer write(AutoBuffer bb) {
      bb.put4(size());
      for(T t:this)
        bb.put(t);
      return bb;
    }
    @Override public IcedArrayList<T> read(AutoBuffer bb) {
      int n = bb.get4();
      for(int i = 0; i < n; ++i)
        add(bb.<T>get());
      return this;
    }

    @Override public <T2 extends Freezable> T2 newInstance() {
      return (T2)new IcedArrayList<T>();
    }
    private static int _frozen$type;
    @Override public int frozenType() {
      return _frozen$type == 0 ? (_frozen$type=water.TypeMap.onIce(IcedArrayList.class.getName())) : _frozen$type;
    }
    @Override public AutoBuffer writeJSONFields(AutoBuffer bb) {
      return bb;
    }
    @Override public FieldDoc[] toDocField() {
      return null;
    }
  }

  public static class IcedInt extends Iced {
    public final int _val;
    public IcedInt(int v){_val = v;}
    @Override public boolean equals( Object o ) {
      if( !(o instanceof IcedInt) ) return false;
      return ((IcedInt)o)._val == _val;
    }
    @Override public int hashCode() { return _val; }
  }
  public static class IcedLong extends Iced {
    public final long _val;
    public IcedLong(long v){_val = v;}
    @Override public boolean equals( Object o ) {
      if( !(o instanceof IcedLong) ) return false;
      return ((IcedLong)o)._val == _val;
    }
    @Override public int hashCode() { return (int)_val; }
  }
  public static class IcedDouble extends Iced {
    public final double _val;
    public IcedDouble(double v){_val = v;}
    @Override public boolean equals( Object o ) {
      if( !(o instanceof IcedDouble) ) return false;
      return ((IcedDouble)o)._val == _val;
    }
    @Override public int hashCode() { return (int)Double.doubleToLongBits(_val); }
  }
  /**
   * Simple wrapper around HashMap with support for H2O serialization
   * @author tomasnykodym
   */
  public static class IcedHashMap<K extends Iced, V extends Iced> extends HashMap<K,V> implements Freezable {
    @Override public AutoBuffer write(AutoBuffer bb) {
      bb.put4(size());
      for( Map.Entry<K, V> e : entrySet() )
        bb.put(e.getKey()).put(e.getValue());
      return bb;
    }
    @Override public IcedHashMap<K,V> read(AutoBuffer bb) {
      int n = bb.get4();
      for(int i = 0; i < n; ++i)
        put(bb.<K>get(),bb.<V>get());
      return this;
    }

    @Override public IcedHashMap<K,V> newInstance() { return new IcedHashMap<K,V>(); }
    private static int _frozen$type;
    @Override public int frozenType() {
      return _frozen$type == 0 ? (_frozen$type=water.TypeMap.onIce(IcedHashMap.class.getName())) : _frozen$type;
    }
    @Override public AutoBuffer writeJSONFields(AutoBuffer bb) { return bb; }
    @Override public FieldDoc[] toDocField() { return null; }
  }
  public static final boolean hasNaNsOrInfs(double [] arr){
    for(double d:arr) if(Double.isNaN(d) || Double.isInfinite(d))return true;
    return false;
  }

  public static class ExpectedExceptionForDebug extends RuntimeException {
  }

  public static String getStackAsString(Throwable t) {
    Writer result = new StringWriter();
    PrintWriter printWriter = new PrintWriter(result);
    t.printStackTrace(printWriter);
    return result.toString();
  }

  /** Returns a mapping of given domain to values (0, ... max(dom)).
   * Unused domain items has mapping to -1.
   * @precondition - dom is sorted dom[0] contains minimal value, dom[dom.length-1] represents max. value. */
  public static int[] mapping(int[] dom) {
    if (dom.length == 0) return new int[] {};
    assert dom[0] <= dom[dom.length-1] : "Domain is not sorted";
    int min = dom[0];
    int max = dom[dom.length-1];
    int[] result = new int[(max-min)+1];
    for (int i=0; i<result.length; i++) result[i] = -1; // not used fields
    for (int i=0; i<dom.length; i++) result[dom[i]-min] = i;
    return result;
  }
  public static String[] toString(long[] dom) {
    String[] result = new String[dom.length];
    for (int i=0; i<dom.length; i++) result[i] = String.valueOf(dom[i]);
    return result;
  }
  public static String[] toString(int[] dom) {
    String[] result = new String[dom.length];
    for (int i=0; i<dom.length; i++) result[i] = String.valueOf(dom[i]);
    return result;
  }
  public static String[] toStringMap(int first, int last) {
    if(first > last) throw new IllegalArgumentException("first must be an integer less than or equal to last");
    String[] result = new String[last-first+1];
    for(int i = first; i <= last; i++) result[i-first] = String.valueOf(i);
    return result;
  }
  public static int[] compose(int[] first, int[] transf) {
    for (int i=0; i<first.length; i++) {
      if (first[i]!=-1) first[i] = transf[first[i]];
    }
    return first;
  }
  public static int[][] compose(int[][] first, int[][] second) {
    int[] firstDom = first[0];
    int[] firstRan = first[1];  // flat transformation
    int[] secondDom = second[0];
    int[] secondRan = second[1];

    boolean[] filter = new boolean[firstDom.length]; int fcnt = 0;
    int[] resDom = firstDom.clone();
    int[] resRan = firstRan!=null ? firstRan.clone() : new int[firstDom.length];
    for (int i=0; i<resDom.length; i++) {
      int v = firstRan!=null ? firstRan[i] : i; // resulting value
      int vi = Arrays.binarySearch(secondDom, v);
      // Do not be too strict in composition assert vi >=0 : "Trying to compose two incompatible transformation: first=" + Arrays.deepToString(first) + ", second=" + Arrays.deepToString(second);
      if (vi<0) {
        filter[i] = true;
        fcnt++;
      } else
        resRan[i] = secondRan!=null ? secondRan[vi] : vi;
    }
    return new int[][] { filter(resDom,filter,fcnt), filter(resRan,filter,fcnt) };
  }

  private static final DecimalFormat default_dformat = new DecimalFormat("0.#####");
  public static String pprint(double[][] arr){
    return pprint(arr,default_dformat);
  }
  // pretty print Matrix(2D array of doubles)
  public static String pprint(double[][] arr,DecimalFormat dformat) {
    int colDim = 0;
    for( double[] line : arr )
      colDim = Math.max(colDim, line.length);
    StringBuilder sb = new StringBuilder();
    int max_width = 0;
    int[] ilengths = new int[colDim];
    Arrays.fill(ilengths, -1);
    for( double[] line : arr ) {
      for( int c = 0; c < line.length; ++c ) {
        double d = line[c];
        String dStr = dformat.format(d);
        if( dStr.indexOf('.') == -1 ) dStr += ".0";
        ilengths[c] = Math.max(ilengths[c], dStr.indexOf('.'));
        int prefix = (d >= 0 ? 1 : 2);
        max_width = Math.max(dStr.length() + prefix, max_width);
      }
    }
    for( double[] line : arr ) {
      for( int c = 0; c < line.length; ++c ) {
        double d = line[c];
        String dStr = dformat.format(d);
        if( dStr.indexOf('.') == -1 ) dStr += ".0";
        for( int x = dStr.indexOf('.'); x < ilengths[c] + 1; ++x )
          sb.append(' ');
        sb.append(dStr);
        if( dStr.indexOf('.') == -1 ) sb.append('.');
        for( int i = dStr.length() - Math.max(0, dStr.indexOf('.')); i <= 5; ++i )
          sb.append('0');
      }
      sb.append("\n");
    }
    return sb.toString();
  }

  static public boolean isEmpty(int[] a) { return a==null || a.length == 0; }
  static public boolean contains(int[] a, int d) { for(int i=0; i<a.length; i++) if (a[i]==d) return true; return false; }
  static public int[] difference(int a[], int b[]) {
    int[] r = new int[a.length];
    int cnt = 0;
    for (int i=0; i<a.length; i++) {
      if (!contains(b, a[i])) r[cnt++] = a[i];
    }
    return Arrays.copyOf(r, cnt);
  }
  /** Generates sequence <start, stop) of integers: (start, start+1, ...., stop-1) */
  static public int[] seq(int start, int stop) {
    assert start<stop;
    int len = stop-start;
    int[] res = new int[len];
    for(int i=start; i<stop;i++) res[i-start] = i;
    return res;
  }

  public static String className(String path) {
    return path.replace('\\', '/').replace('/', '.').substring(0, path.length() - 6);
  }

  public static double avg(double[] nums) {
    double sum = 0;
    for(double n: nums) sum+=n;
    return sum/nums.length;
  }
  public static double avg(long[] nums) {
    long sum = 0;
    for(long n: nums) sum+=n;
    return sum/nums.length;
  }
  public static float[] div(float[] nums, int n) {
    for (int i=0; i<nums.length; i++) nums[i] /= n;
    return nums;
  }
  public static float[] div(float[] nums, float n) {
    assert !Float.isInfinite(n) : "Trying to divide " + Arrays.toString(nums) + " by  " + n; // Almost surely not what you want
    for (int i=0; i<nums.length; i++) nums[i] /= n;
    return nums;
  }
  public static double[] div(double[] nums, double n) {
    assert !Double.isInfinite(n) : "Trying to divide " + Arrays.toString(nums) + " by  " + n; // Almost surely not what you want
    for (int i=0; i<nums.length; i++) nums[i] /= n;
    return nums;
  }
  public static float[] mult(float[] nums, float n) {
    assert !Float.isInfinite(n) : "Trying to multiply " + Arrays.toString(nums) + " by  " + n; // Almost surely not what you want
    for (int i=0; i<nums.length; i++) nums[i] *= n;
    return nums;
  }
  public static double[] mult(double[] nums, double n) {
    assert !Double.isInfinite(n) : "Trying to multiply " + Arrays.toString(nums) + " by  " + n; // Almost surely not what you want
    for (int i=0; i<nums.length; i++) nums[i] *= n;
    return nums;
  }

  /**
   * Fast approximate sqrt
   * @param x
   * @return sqrt(x) with up to 5% relative error
   */
  public static double approxSqrt(double x) {
    return Double.longBitsToDouble(((Double.doubleToLongBits(x) >> 32) + 1072632448) << 31);
  }
  /**
   * Fast approximate sqrt
   * @param x
   * @return sqrt(x) with up to 5% relative error
   */
  public static float approxSqrt(float x) {
    return Float.intBitsToFloat(532483686 + (Float.floatToRawIntBits(x) >> 1));
  }
  /**
   * Fast approximate 1./sqrt
   * @param x
   * @return 1./sqrt(x) with up to 2% relative error
   */
  public static double approxInvSqrt(double x) {
    double xhalf = 0.5d*x; x = Double.longBitsToDouble(0x5fe6ec85e7de30daL - (Double.doubleToLongBits(x)>>1)); return x*(1.5d - xhalf*x*x);
  }
  /**
   * Fast approximate 1./sqrt
   * @param x
   * @return 1./sqrt(x) with up to 2% relative error
   */
  public static float approxInvSqrt(float x) {
    float xhalf = 0.5f*x; x = Float.intBitsToFloat(0x5f3759df - (Float.floatToIntBits(x)>>1)); return x*(1.5f - xhalf*x*x);
  }
  /**
   * Fast approximate exp
   * @param x
   * @return exp(x) with up to 5% relative error
   */
  public static double approxExp(double x) {
    return Double.longBitsToDouble(((long)(1512775 * x + 1072632447)) << 32);
  }
  /**
   * Fast approximate log for values > 1, otherwise exact
   * @param x
   * @return log(x) with up to 0.1% relative error
   */
  public static double approxLog(double x){
    if (x > 1) return ((Double.doubleToLongBits(x) >> 32) - 1072632447d) / 1512775d;
    else return Math.log(x);
  }

  public static class approxMathTester {
    @Test
    public void run() {
      final int loops = 100000;
      for (float maxVal : new float[]{1, Float.MAX_VALUE}) {
        Log.info("Testing " + loops + " numbers in interval [0, " + maxVal + "].");
        // float square
        {
          float err = 0;
          for (int i=0;i<loops;++i) {
            final float x = new Random().nextFloat() * maxVal;
            err = Math.max(Math.abs(err), Math.abs((float)Math.sqrt(x)-approxSqrt(x))/(float)Math.sqrt(x));
          }
          Log.info("rel. error for approxSqrt(float): " + err);
          Assert.assertTrue("rel. error for approxSqrt(float): " + err, Math.abs(err) < 5e-2);
        }

        // double square
        {
          double err = 0;
          for (int i=0;i<loops;++i) {
            final double x = new Random().nextFloat() * maxVal;
            err = Math.max(Math.abs(err), Math.abs(Math.sqrt(x)-approxSqrt(x))/Math.sqrt(x));
          }
          Log.info("rel. error for approxSqrt(double): " + err);
          Assert.assertTrue("rel. error for approxSqrt(double): " + err, Math.abs(err) < 5e-2);
        }

        // float inv square
        {
          float err = 0;
          for (int i=0;i<loops;++i) {
            final float x = new Random().nextFloat() * maxVal;
            err = Math.max(Math.abs(err), Math.abs((float)(1./Math.sqrt(x))-approxInvSqrt(x))*(float)Math.sqrt(x));
          }
          Log.info("rel. error for approxInvSqrt(float): " + err);
          Assert.assertTrue("rel. error for approxInvSqrt(float): " + err, Math.abs(err) < 2e-2);
        }

        // double inv square
        {
          double err = 0;
          for (int i=0;i<loops;++i) {
            final double x = new Random().nextFloat() * maxVal;
            err = Math.max(Math.abs(err), Math.abs((1./Math.sqrt(x))-approxInvSqrt(x))*Math.sqrt(x));
          }
          Log.info("rel. error for approxInvSqrt(double): " + err);
          Assert.assertTrue("rel. error for approxInvSqrt(double): " + err, Math.abs(err) < 2e-2);
        }

        // double exp
        {
          double err = 0;
          for (int i=0;i<loops;++i) {
            final double x = 30 - new Random().nextDouble() * 60;
            err = Math.max(Math.abs(err), Math.abs(Math.exp(x)-approxExp(x))/Math.exp(x));
          }
          Log.info("rel. error for approxExp(double): " + err);
          Assert.assertTrue("rel. error for approxExp(double): " + err, Math.abs(err) < 5e-2);
        }

        // double log
        {
          double err = 0;
          for (int i=0;i<loops;++i) {
            final double x = new Random().nextFloat() * maxVal;
            err = Math.max(err, Math.abs(Math.log(x)-approxLog(x))/Math.abs(Math.log(x)));
          }
          Log.info("rel. error for approxLog(double): " + err);
          Assert.assertTrue("rel. error for approxLog(double): " + err, Math.abs(err) < 1e-3);
        }
      }
    }
  }

  /**
   * Replace given characters in a given string builder.
   * The number of characters to replace has to match to number of
   * characters serving as a replacement.
   *
   * @param sb string builder containing a string to be modified
   * @param from characters to replaced
   * @param to replacement characters
   * @return original string builder with replaced characters.
   */
  public static StringBuilder replace(StringBuilder sb, CharSequence from, CharSequence to) {
    assert from.length() == to.length();
    for (int i=0; i<sb.length(); i++)
      for (int j=0; j<from.length(); j++)
        if (sb.charAt(i)==from.charAt(j)) sb.setCharAt(i, to.charAt(j));
    return sb;
  }

  /**
   * Returns true if given string contains at least on of character of
   * given sequence.
   * @param s string
   * @param cs a sequence of character
   * @return true if s contains at least one of character from given sequence, else false
   */
  public static boolean contains(String s, CharSequence cs) {
    for (int i=0; i<s.length(); i++)
      for (int j=0; j<cs.length(); j++)
        if (s.charAt(i) == cs.charAt(j)) return true;
    return false;
  }


  // Atomically-updated float array
  public static class AtomicFloatArray {
    private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
    private static final int _Fbase  = _unsafe.arrayBaseOffset(float[].class);
    private static final int _Fscale = _unsafe.arrayIndexScale(float[].class);
    private static long rawIndex(final float[] ary, final int idx) {
      assert idx >= 0 && idx < ary.length;
      return _Fbase + idx * _Fscale;
    }
    static public void setMin( float fs[], int i, float min ) {
      float old = fs[i];
      while( min < old && !_unsafe.compareAndSwapInt(fs,rawIndex(fs,i), Float.floatToRawIntBits(old), Float.floatToRawIntBits(min) ) )
        old = fs[i];
    }
    static public void setMax( float fs[], int i, float max ) {
      float old = fs[i];
      while( max > old && !_unsafe.compareAndSwapInt(fs,rawIndex(fs,i), Float.floatToRawIntBits(old), Float.floatToRawIntBits(max) ) )
        old = fs[i];
    }
    static public String toString( float fs[] ) {
      SB sb = new SB();
      sb.p('[');
      for( float f : fs )
        sb.p(f==Float.MAX_VALUE ? "max": (f==-Float.MAX_VALUE ? "min": Float.toString(f))).p(',');
      return sb.p(']').toString();
    }
  }

  // Atomically-updated double array
  public static class AtomicDoubleArray {
    private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
    private static final int _Dbase  = _unsafe.arrayBaseOffset(double[].class);
    private static final int _Dscale = _unsafe.arrayIndexScale(double[].class);
    private static long rawIndex(final double[] ary, final int idx) {
      assert idx >= 0 && idx < ary.length;
      return _Dbase + idx * _Dscale;
    }
    static public void add( double ds[], int i, double y ) {
      long adr = rawIndex(ds,i);
      double old = ds[i];
      while( !_unsafe.compareAndSwapLong(ds,adr, Double.doubleToRawLongBits(old), Double.doubleToRawLongBits(old+y) ) )
        old = ds[i];
    }
  }

  // Atomically-updated long array.  Instead of using the similar JDK pieces,
  // allows the bare array to be exposed for fast readers.
  public static class AtomicLongArray {
    private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
    private static final int _Lbase  = _unsafe.arrayBaseOffset(long[].class);
    private static final int _Lscale = _unsafe.arrayIndexScale(long[].class);
    private static long rawIndex(final long[] ary, final int idx) {
      assert idx >= 0 && idx < ary.length;
      return _Lbase + idx * _Lscale;
    }
    static public void incr( long ls[], int i ) {
      long adr = rawIndex(ls,i);
      long old = ls[i];
      while( !_unsafe.compareAndSwapLong(ls,adr, old, old+1) )
        old = ls[i];
    }
  }
  // Atomically-updated int array.  Instead of using the similar JDK pieces,
  // allows the bare array to be exposed for fast readers.
  public static class AtomicIntArray {
    private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
    private static final int _Ibase  = _unsafe.arrayBaseOffset(int[].class);
    private static final int _Iscale = _unsafe.arrayIndexScale(int[].class);
    private static long rawIndex(final int[] ary, final int idx) {
      assert idx >= 0 && idx < ary.length;
      return _Ibase + idx * _Iscale;
    }
    static public void incr( int is[], int i ) { add(is,i,1); }
    static public void add( int is[], int i, int x ) {
      long adr = rawIndex(is,i);
      int old = is[i];
      while( !_unsafe.compareAndSwapInt(is,adr, old, old+x) )
        old = is[i];
    }
  }

  public static boolean contains(String[] names, String name) {
    for (String n : names) if (n.equals(name)) return true;
    return false;
  }

  /** Java-string illegal characters which need to be escaped */
  public static final Pattern[] ILLEGAL_CHARACTERS = new Pattern[] { Pattern.compile("\\",Pattern.LITERAL), Pattern.compile("\"",Pattern.LITERAL) };
  public static final String[]  REPLACEMENTS       = new String [] { "\\\\\\\\", "\\\\\"" };

  /** Escape all " and \ characters to provide a proper Java-like string
   * Does not escape unicode characters.
   */
  public static String escapeJava(String s) {
    assert ILLEGAL_CHARACTERS.length == REPLACEMENTS.length;
    for (int i=0; i<ILLEGAL_CHARACTERS.length; i++ ) {
      Matcher m = ILLEGAL_CHARACTERS[i].matcher(s);
      s = m.replaceAll(REPLACEMENTS[i]);
    }
    return s;
  }

  /** Clever union of String arrays.
   *
   * For union of numeric arrays (strings represent integers) it is expecting numeric ordering.
   * For pure string domains it is expecting lexicographical ordering.
   * For mixed domains it always expects lexicographical ordering since such a domain were produce
   * by a parser which sort string with Array.sort().
   *
   * @PRECONDITION - string domain was sorted by Array.sort(String[]), integer domain by Array.sort(int[]) and switched to Strings !!!
   *
   * @param a a set of strings
   * @param b a set of strings
   * @return union of arrays
   */
  public static String[] domainUnion(String[] a, String[] b) {
    int cIinA = numInts(a);
    int cIinB = numInts(b);
    // Trivial case - all strings or ints, sorted
    if (cIinA==0 && cIinB==0   // only strings
        || cIinA==a.length && cIinB==b.length ) // only integers
      return union(a, b, cIinA==0);
    // Be little bit clever here: sort string representing numbers first and append
    // a,b were sorted by Array.sort() but can contain some numbers.
    // So sort numbers in numeric way, and then string in lexicographical order
    int[] ai = toInt(a, 0, cIinA); Arrays.sort(ai); // extract int part but sort it in numeric order
    int[] bi = toInt(b, 0, cIinB); Arrays.sort(bi);
    String[] ri = toString(union(ai,bi)); // integer part
    String[] si = union(a,b,cIinA,a.length-cIinA,cIinB,b.length-cIinB,true);
    return join(ri, si);
  }

  /** Union of given String arrays.
   *
   * The method expects ordering of domains in given order (lexicographical, numeric)
   *
   * @param a first array
   * @param b second array
   * @param lexo - true if domains are sorted in lexicographical order or false for numeric domains
   * @return union of values in given arrays.
   *
   * @precondition lexo ? a,b are lexicographically sorted : a,b are sorted numerically
   * @precondition a!=null && b!=null
   */
  public static String[] union(String[] a, String[] b, boolean lexo) {
    assert a!=null && b!=null : "Union expect non-null input!";
    return union(a, b, 0, a.length, 0, b.length, lexo);
  }
  public static String[] union(String[] a, String[] b, int aoff, int alen, int boff, int blen, boolean lexo) {
    assert a!=null && b!=null : "Union expect non-null input!";
    String[] r = new String[alen+blen];
    int ia = aoff, ib = boff, i = 0;
    while (ia < aoff+alen && ib < boff+blen) {
      int c = lexo ? a[ia].compareTo(b[ib]) : Integer.valueOf(a[ia]).compareTo(Integer.valueOf(b[ib]));
      if ( c < 0) r[i++] = a[ia++];
      else if (c == 0) { r[i++] = a[ia++]; ib++; }
      else r[i++] = b[ib++];
    }
    if (ia < aoff+alen) while (ia<aoff+alen) r[i++] = a[ia++];
    if (ib < boff+blen) while (ib<boff+blen) r[i++] = b[ib++];
    return Arrays.copyOf(r, i);
  }

  /** Returns a union of given sorted arrays. */
  public static int[] union(int[] a, int[] b) {
    assert a!=null && b!=null : "Union expect non-null input!";
    int[] r = new int[a.length+b.length];
    int ia = 0, ib = 0, i = 0;
    while (ia < a.length && ib < b.length) {
      int c = a[ia]-b[ib];
      if ( c < 0) r[i++] = a[ia++];
      else if (c == 0) { r[i++] = a[ia++]; ib++; }
      else r[i++] = b[ib++];
    }
    if (ia < a.length) while (ia<a.length) r[i++] = a[ia++];
    if (ib < b.length) while (ib<b.length) r[i++] = b[ib++];
    return Arrays.copyOf(r, i);
  }

  public static <T> T[] join(T[] a, T[] b) {
    T[] res = Arrays.copyOf(a, a.length+b.length);
    System.arraycopy(b, 0, res, a.length, b.length);
    return res;
  }

  /** Returns number of strings which represents a number. */
  public static int numInts(String... a) {
    int cnt = 0;
    for(String s : a) if (isInt(s)) cnt++;
    return cnt;
  }

  public static boolean isInt(String s) {
    int i = s.charAt(0)=='-' ? 1 : 0;
    for(; i<s.length();i++) if (!Character.isDigit(s.charAt(i))) return false;
    return true;
  }

  public static int[] toInt(String[] a, int off, int len) {
    int[] res = new int[len];
    for(int i=0; i<len; i++) res[i] = Integer.valueOf(a[off+i]);
    return res;
  }

  public static int[] filter(int[] values, boolean[] filter, int fcnt) {
    assert filter.length == values.length : "Values should have same length as filter!";
    assert filter.length - fcnt >= 0 : "Cannot filter more values then legth of filter vector!";
    if (fcnt==0) return values;
    int[] result = new int[filter.length - fcnt];
    int c = 0;
    for (int i=0; i<values.length; i++) {
      if (!filter[i]) result[c++] = values[i];
    }
    return result;
  }

  public static int[][] pack(int[] values, boolean[] usemap) {
    assert values.length == usemap.length : "Cannot pack the map according given use map!";
    int cnt = 0;
    for (int i=0; i<usemap.length; i++) cnt += usemap[i] ? 1 : 0;
    int[] pvals = new int[cnt]; // only used values
    int[] pindx = new int[cnt]; // indexes of used values
    int index = 0;
    for (int i=0; i<usemap.length; i++) {
      if (usemap[i]) {
        pvals[index] = values[i];
        pindx[index] = i;
        index++;
      }
    }
    return new int[][] { pvals, pindx };
  }

  /**
   * Poisson-distributed RNG
   * @param lambda Lambda parameter
   * @return Poisson-distributed random number in [0,inf)
   */
  public static int getPoisson(double lambda, Random rng) {
    double L = Math.exp(-lambda);
    double p = 1.0;
    int k = 0;
    if (rng == null) rng = new Random();
    do {
      k++;
      p *= rng.nextDouble();
    } while (p > L);
    return k - 1;
  }

  /** Create a new sorted array according to given sort order */
  public static float[] sortAccording(float[] ary, Integer[] sortOrder) {
    float[] res = new float[ary.length];
    for(int i=0; i<ary.length; i++) res[i] = ary[sortOrder[i]];
    return res;
  }
  public static String[] sortAccording(String[] ary, Integer[] sortOrder) {
    String[] res = new String[ary.length];
    for(int i=0; i<ary.length; i++) res[i] = ary[sortOrder[i]];
    return res;
  }
  public static int[] sortAccording(int[] ary, Integer[] sortOrder) {
    int[] res = new int[ary.length];
    for(int i=0; i<ary.length; i++) res[i] = ary[sortOrder[i]];
    return res;
  }
  /** Sort two arrays - the second one is sorted according the first one. */
  public static void sortWith(final int[] ary, int[] ary2) {
    Integer[] sortOrder = new Integer[ary.length];
    for(int i=0; i<sortOrder.length; i++) sortOrder[i] = i;
    Arrays.sort(sortOrder, new Comparator<Integer>() {
      @Override public int compare(Integer o1, Integer o2) { return ary[o1]-ary[o2]; }
    });
    sortAccording2(ary,  sortOrder);
    sortAccording2(ary2, sortOrder);
  }
  /** Sort given array according given sort order. Sort is implemented in-place. */
  public static void sortAccording2(int[] ary, Integer[] sortOrder) {
    Integer[] so = sortOrder.clone(); // we are modifying sortOrder to preserve exchanges
    for(int i=0; i<ary.length; i++) {
      int tmp = ary[i];
      int idx = so[i];
      ary[i] = ary[idx];
      ary[idx] = tmp;
      for (int j=i; j<so.length; j++) if (so[j]==i) { so[j] = idx; break; }
    }
  }
  /** Sort given array according given sort order. Sort is implemented in-place. */
  public static void sortAccording2(boolean[] ary, Integer[] sortOrder) {
    Integer[] so = sortOrder.clone(); // we are modifying sortOrder to preserve exchanges
    for(int i=0; i<ary.length; i++) {
      boolean tmp = ary[i];
      int idx = so[i];
      ary[i] = ary[idx];
      ary[idx] = tmp;
      for (int j=i; j<so.length; j++) if (so[j]==i) { so[j] = idx; break; }
    }
  }

  public static String[] createConfusionMatrixHeader( long xs[], String ds[] ) {
    String ss[] = new String[xs.length]; // the same length
    for( int i=0; i<ds.length; i++ )
      if( xs[i] >= 0 || (ds[i] != null && ds[i].length() > 0) && !Integer.toString(i).equals(ds[i]) )
        ss[i] = ds[i];
    if( ds.length == xs.length-1 && xs[xs.length-1] > 0 )
      ss[xs.length-1] = "NA";
    return ss;
  }

  public static void printConfusionMatrix(StringBuilder sb, long[][] cm, String[] domain, boolean html) {
    assert(cm != null);
    assert(domain != null);
    for (int i=0; i<cm.length; ++i) assert(cm.length == cm[i].length);
    if (html) DocGen.HTML.arrayHead(sb);
    // Sum up predicted & actuals
    long acts [] = new long[cm   .length];
    long preds[] = new long[cm[0].length];
    for( int a=0; a<cm.length; a++ ) {
      long sum=0;
      for( int p=0; p<cm[a].length; p++ ) {
        sum += cm[a][p];
        preds[p] += cm[a][p];
      }
      acts[a] = sum;
    }
    String adomain[] = createConfusionMatrixHeader(acts , domain);
    String pdomain[] = createConfusionMatrixHeader(preds, domain);
    assert adomain.length == pdomain.length : "The confusion matrix should have the same length for both directions.";

    String fmt = "";
    String fmtS = "";

    // Header
    if (html) {
      sb.append("<tr class='warning' style='min-width:60px'>");
      sb.append("<th>Actual / Predicted</th>");
      for( int p=0; p<pdomain.length; p++ )
        if( pdomain[p] != null )
          sb.append("<th style='min-width:60px'>").append(pdomain[p]).append("</th>");
      sb.append("<th>Error</th>");
      sb.append("</tr>");
    } else {
      // determine max length of each space-padded field
      int maxlen = 0;
      for( String s : pdomain ) if( s != null ) maxlen = Math.max(maxlen, s.length());
      long lsum = 0;
      for( int a=0; a<cm.length; a++ ) {
        if( adomain[a] == null ) continue;
        for( int p=0; p<pdomain.length; p++ ) { if( pdomain[p] == null ) continue; lsum += cm[a][p]; }
      }
      maxlen = Math.max(8, Math.max(maxlen, String.valueOf(lsum).length()) + 2);
      fmt  = "%" + maxlen + "d";
      fmtS = "%" + maxlen + "s";
      sb.append(String.format(fmtS, "Act/Prd"));
      for( String s : pdomain ) if( s != null ) sb.append(String.format(fmtS, s));
      sb.append("   " + String.format(fmtS, "Error\n"));
    }

    // Main CM Body
    long terr=0;
    for( int a=0; a<cm.length; a++ ) {
      if( adomain[a] == null ) continue;
      if (html) {
        sb.append("<tr style='min-width:60px'>");
        sb.append("<th style='min-width:60px'>").append(adomain[a]).append("</th>");
      } else {
        sb.append(String.format(fmtS,adomain[a]));
      }
      long correct=0;
      for( int p=0; p<pdomain.length; p++ ) {
        if( pdomain[p] == null ) continue;
        boolean onDiag = adomain[a].equals(pdomain[p]);
        if( onDiag ) correct = cm[a][p];
        String id = "";
        if (html) {
          sb.append(onDiag ? "<td style='min-width: 60px; background-color:LightGreen' "+id+">":"<td style='min-width: 60px;'"+id+">").append(String.format("%,d", cm[a][p])).append("</td>");
        } else {
          sb.append(String.format(fmt,cm[a][p]));
        }
      }
      long err = acts[a]-correct;
      terr += err;
      if (html) {
        sb.append(String.format("<th  style='min-width: 60px;'>%5.3f = %,d / %,d</th></tr>", (double)err/acts[a], err, acts[a]));
      } else {
        sb.append("   " + String.format("%5.3f = %,d / %d\n", (double)err/acts[a], err, acts[a]));
      }
    }

    // Last row of CM
    if (html) {
      sb.append("<tr style='min-width:60px'><th>Totals</th>");
    } else {
      sb.append(String.format(fmtS, "Totals"));
    }
    for( int p=0; p<pdomain.length; p++ ) {
      if( pdomain[p] == null ) continue;
      if (html) {
        sb.append("<td style='min-width:60px'>").append(String.format("%,d", preds[p])).append("</td>");
      } else {
        sb.append(String.format(fmt, preds[p]));
      }
    }
    long nrows = 0;
    for (long n : acts) nrows += n;

    if (html) {
      sb.append(String.format("<th style='min-width:60px'>%5.3f = %,d / %,d</th></tr>", (float)terr/nrows, terr, nrows));
      DocGen.HTML.arrayTail(sb);
    } else {
      sb.append("   " + String.format("%5.3f = %,d / %,d\n", (float)terr/nrows, terr, nrows));
    }
  }
}
