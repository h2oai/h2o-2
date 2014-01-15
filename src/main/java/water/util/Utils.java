package water.util;

import hex.rng.*;
import hex.rng.H2ORandomRNG.RNGKind;
import hex.rng.H2ORandomRNG.RNGType;

import java.io.*;
import java.net.Socket;
import java.net.URL;
import java.security.SecureRandom;
import java.text.DecimalFormat;
import java.util.*;
import java.util.zip.*;

import sun.misc.Unsafe;
import water.*;
import water.api.DocGen.FieldDoc;
import water.nbhm.UtilUnsafe;
import water.parser.ParseDataset.Compression;
import water.parser.ParseDataset;
import water.parser.ValueString;

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

  public static int sum(int[] from) {
    int result = 0;
    for (int d: from) result += d;
    return result;
  }
  public static float sum(float[] from) {
    float result = 0;
    for (float d: from) result += d;
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
  /**
   * Simple wrapper around HashMap with support for H2O serialization
   * @author tomasnykodym
   * @param <T>
   */
  public static class IcedHashMap<K extends Iced, V extends Iced> extends HashMap<K,V> implements Freezable {
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
    private static int _frozen$type;
    @Override public int frozenType() {
      return _frozen$type == 0 ? (_frozen$type=water.TypeMap.onIce(IcedHashMap.class.getName())) : _frozen$type;
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

  // Deduce if we are looking at a Date/Time value, or not.
  // If so, return time as msec since Jan 1, 1970 or Long.MIN_VALUE.

  // I tried java.util.SimpleDateFormat, but it just throws too many
  // exceptions, including ParseException, NumberFormatException, and
  // ArrayIndexOutOfBoundsException... and the Piece de resistance: a
  // ClassCastException deep in the SimpleDateFormat code:
  // "sun.util.calendar.Gregorian$Date cannot be cast to sun.util.calendar.JulianCalendar$Date"
  public static int digit( int x, int c ) {
    if( x < 0 || c < '0' || c > '9' ) return -1;
    return x*10+(c-'0');
  }

  // So I just brutally parse "dd-MMM-yy".
  public static final byte MMS[][][] = new byte[][][] {
    {"jan".getBytes(),null},
    {"feb".getBytes(),null},
    {"mar".getBytes(),null},
    {"apr".getBytes(),null},
    {"may".getBytes(),null},
    {"jun".getBytes(),"june".getBytes()},
    {"jul".getBytes(),"july".getBytes()},
    {"aug".getBytes(),null},
    {"sep".getBytes(),"sept".getBytes()},
    {"oct".getBytes(),null},
    {"nov".getBytes(),null},
    {"dec".getBytes(),null}
  };

  public static long attemptTimeParse( ValueString str ) {
    long t0 = attemptTimeParse_0(str); // "yyyy-MM-dd HH:mm:ss.SSS"
    if( t0 != Long.MIN_VALUE ) return t0;
    long t1 = attemptTimeParse_1(str); // "dd-MMM-yy"
    if( t1 != Long.MIN_VALUE ) return t1;
    return Long.MIN_VALUE;
  }
  // So I just brutally parse "yyyy-MM-dd HH:mm:ss.SSS"
  private static long attemptTimeParse_0( ValueString str ) {
    final byte[] buf = str.get_buf();
    int i=str.get_off();
    final int end = i+str.get_length();
    while( i < end && buf[i] == ' ' ) i++;
    if   ( i < end && buf[i] == '"' ) i++;
    if( (end-i) < 19 ) return Long.MIN_VALUE;
    int yy=0, MM=0, dd=0, HH=0, mm=0, ss=0, SS=0;
    yy = digit(yy,buf[i++]);
    yy = digit(yy,buf[i++]);
    yy = digit(yy,buf[i++]);
    yy = digit(yy,buf[i++]);
    if( yy < 1970 ) return Long.MIN_VALUE;
    if( buf[i++] != '-' ) return Long.MIN_VALUE;
    MM = digit(MM,buf[i++]);
    MM = digit(MM,buf[i++]);
    if( MM < 1 || MM > 12 ) return Long.MIN_VALUE;
    if( buf[i++] != '-' ) return Long.MIN_VALUE;
    dd = digit(dd,buf[i++]);
    dd = digit(dd,buf[i++]);
    if( dd < 1 || dd > 31 ) return Long.MIN_VALUE;
    if( buf[i++] != ' ' ) return Long.MIN_VALUE;
    HH = digit(HH,buf[i++]);
    HH = digit(HH,buf[i++]);
    if( HH < 0 || HH > 23 ) return Long.MIN_VALUE;
    if( buf[i++] != ':' ) return Long.MIN_VALUE;
    mm = digit(mm,buf[i++]);
    mm = digit(mm,buf[i++]);
    if( mm < 0 || mm > 59 ) return Long.MIN_VALUE;
    if( buf[i++] != ':' ) return Long.MIN_VALUE;
    ss = digit(ss,buf[i++]);
    ss = digit(ss,buf[i++]);
    if( ss < 0 || ss > 59 ) return Long.MIN_VALUE;
    if( i<end && buf[i] == '.' ) {
      i++;
      if( i<end ) SS = digit(SS,buf[i++]);
      if( i<end ) SS = digit(SS,buf[i++]);
      if( i<end ) SS = digit(SS,buf[i++]);
      if( SS < 0 || SS > 999 ) return Long.MIN_VALUE;
    }
    if( i<end && buf[i] == '"' ) i++;
    if( i<end ) return Long.MIN_VALUE;
    return new GregorianCalendar(yy,MM,dd,HH,mm,ss).getTimeInMillis()+SS;
  }

  private static long attemptTimeParse_1( ValueString str ) {
    final byte[] buf = str.get_buf();
    int i=str.get_off();
    final int end = i+str.get_length();
    while( i < end && buf[i] == ' ' ) i++;
    if   ( i < end && buf[i] == '"' ) i++;
    if( (end-i) < 8 ) return Long.MIN_VALUE;
    int yy=0, MM=0, dd=0;
    dd = digit(dd,buf[i++]);
    if( buf[i] != '-' ) dd = digit(dd,buf[i++]);
    if( dd < 1 || dd > 31 ) return Long.MIN_VALUE;
    if( buf[i++] != '-' ) return Long.MIN_VALUE;
    byte[]mm=null;
    OUTER: for( ; MM<MMS.length; MM++ ) {
      byte[][] mms = MMS[MM];
      INNER: for( int k=0; k<mms.length; k++ ) {
        mm = mms[k];
        if( mm == null ) continue;
        for( int j=0; j<mm.length; j++ )
          if( mm[j] != Character.toLowerCase(buf[i+j]) )
            continue INNER;
        break OUTER;
      }
    }
    if( MM == MMS.length ) return Long.MIN_VALUE; // No matching month
    i += mm.length;             // Skip month bytes
    MM++;                       // 1-based month
    if( buf[i++] != '-' ) return Long.MIN_VALUE;
    yy = digit(yy,buf[i++]);
    yy = digit(yy,buf[i++]);
    yy += 2000;                 // Y2K bug
    if( i<end && buf[i] == '"' ) i++;
    if( i<end ) return Long.MIN_VALUE;
    return new GregorianCalendar(yy,MM,dd).getTimeInMillis();
  }

  /** Returns a mapping of given domain to values (0, ... max(dom)).
   * Unused domain items has mapping to -1.
   * @precondition - dom is sorted dom[0] contains minimal value, dom[dom.length-1] represents max. value. */
  public static int[] mapping(int[] dom) {
    assert dom.length > 0 : "Empty domain!";
    assert dom[0] <= dom[dom.length-1] : "Domain is not sorted";
    int min = dom[0];
    int max = dom[dom.length-1];
    int[] result = new int[(max-min)+1];
    for (int i=0; i<result.length; i++) result[i] = -1; // not used fields
    for (int i=0; i<dom.length; i++) result[dom[i]-min] = i;
    return result;
  }
  public static String[] toStringMap(int[] dom) {
    String[] result = new String[dom.length];
    for (int i=0; i<dom.length; i++) result[i] = String.valueOf(dom[i]);
    return result;
  }
  public static int[] compose(int[] first, int[] transf) {
    for (int i=0; i<first.length; i++) {
      if (first[i]!=-1) first[i] = transf[first[i]];
    }
    return first;
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
    for (int i=0; i<nums.length; i++) nums[i] = nums[i] / n;
    return nums;
  }
  public static float[] div(float[] nums, float n) {
    assert !Float.isInfinite(n); // Almost surely not what you want
    for (int i=0; i<nums.length; i++) nums[i] = nums[i] / n;
    return nums;
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
}
