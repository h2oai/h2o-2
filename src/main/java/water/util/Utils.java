package water.util;

import hex.rng.*;
import hex.rng.H2ORandomRNG.RNGKind;
import hex.rng.H2ORandomRNG.RNGType;

import java.io.*;
import java.net.Socket;
import java.security.SecureRandom;
import java.text.DecimalFormat;
import java.util.*;
import java.util.zip.*;

import org.apache.commons.lang.ArrayUtils;

import water.*;
import water.api.DocGen.FieldDoc;
import water.parser.ParseDataset;
import water.parser.ParseDataset.Compression;

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

  public static int sum(int[] from) {
    int result = 0;
    for (int d: from) result += d;
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

  public static void shuffleArray(long[] a) {
    int n = a.length;
    Random random = new Random();
    random.nextInt();
    for (int i = 0; i < n; i++) {
      int change = i + random.nextInt(n - i);
      swap(a, i, change);
    }
  }

  private static void swap(long[] a, int i, int change) {
    long helper = a[i];
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

  public static String join(char sep, Object[] array) {
    return join(sep, Arrays.asList(array));
  }

  public static String join(char sep, Iterable it) {
    String s = "";
    for( Object o : it )
      s += (s.length() == 0 ? "" : sep) + o.toString();
    return s;
  }

  public static <T> T[] add(T[] a, T... b) {
    return (T[]) ArrayUtils.addAll(a, b);
  }

  public static <T> T[] remove(T[] a, int i) {
    return (T[]) ArrayUtils.remove(a, i);
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
    for(int i = 0; i < a.length; i++ ) a[i] += b[i];
    return a;
  }
  public static long[][] add(long[][] a, long[][] b) {
    for(int i = 0; i < a.length; i++ ) add(a[i],b[i]);
    return a;
  }
  public static float[] add(float[] a, float[] b) {
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

  public static <T> T[] subarray(T[] a, int off, int len) {
    return (T[]) ArrayUtils.subarray(a, off, off + len);
  }

  public static int[] append(int[] a, int[] b) {
    int[] res = new int[a.length + b.length];
    System.arraycopy(a, 0, res, 0, a.length);
    System.arraycopy(b, 0, res, a.length, b.length);
    return res;
  }

  public static double[][] append(double[][] a, double[][] b) {
    double[][] res = new double[a.length + b.length][];
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

  public static ValueArray loadAndParseKey(String path) {
    return loadAndParseKey(Key.make(), path);
  }

  public static ValueArray loadAndParseKey(Key okey, String path) {
    FileIntegrityChecker c = FileIntegrityChecker.check(new File(path),false);
    Futures fs = new Futures();
    Key k = c.importFile(0, fs);
    fs.blockForPending();
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
        if( ze != null && !ze.isDirectory() )
          is = zis;
        else
          zis.close();
        break;
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
    private static final int I;
    static {
      I = TypeMap.onLoad(IcedArrayList.class.getName());
    }
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
    @Override public int frozenType() {
      return I;
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
  }
  /**
   * Simple wrapper around HashMap with support for H2O serialization
   * @author tomasnykodym
   * @param <T>
   */
  public static class IcedHashMap<K extends Iced, V extends Iced> extends HashMap<K,V> implements Freezable {
    private static final int I;
    static {
      I = TypeMap.onLoad(IcedHashMap.class.getName());
    }
    @Override public AutoBuffer write(AutoBuffer bb) {
      bb.put4(size());
      for(Map.Entry<K, V> e:entrySet())bb.put(e.getKey()).put(e.getValue());
      return bb;
    }
    @Override public IcedHashMap<K,V> read(AutoBuffer bb) {
      int n = bb.get4();
      for(int i = 0; i < n; ++i)
        put(bb.<K>get(),bb.<V>get());
      return this;
    }

    @Override public <T2 extends Freezable> T2 newInstance() {
      return (T2)new IcedHashMap<K,V>();
    }
    @Override public int frozenType() {
      return I;
    }
    @Override public AutoBuffer writeJSONFields(AutoBuffer bb) {
      return bb;
    }
    @Override public FieldDoc[] toDocField() {
      return null;
    }
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
}
