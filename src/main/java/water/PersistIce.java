package water;

import java.io.*;
import java.util.Arrays;

// Persistence backend for the local storage device
//
// Stores all keys as files, or if leveldb is enabled, stores values smaller
// than arraylet chunk size in a leveldb format.
//
// Metadata stored are the value type and the desired replication factor of the
// key.
//
// @author peta, cliffc
public abstract class PersistIce {

  // initialization routines ---------------------------------------------------

  protected static final String ROOT;
  public static final String DEFAULT_ROOT = "/tmp";
  private static final String ICE_DIR = "ice";
  private static final File iceRoot;

  // Load into the K/V store all the files found on the local disk
  static void initialize() {}
  static {
    ROOT = (H2O.OPT_ARGS.ice_root==null) ? DEFAULT_ROOT : H2O.OPT_ARGS.ice_root;
    H2O.OPT_ARGS.ice_root = ROOT;
    iceRoot = new File(ROOT+File.separator+ICE_DIR+H2O.API_PORT);
    // Make the directory as-needed
    iceRoot.mkdirs();
    if( !(iceRoot.isDirectory() && iceRoot.canRead() && iceRoot.canWrite()) )
      Log.die("ice_root not a read/writable directory");
    // By popular demand, clear out ICE on startup instead of trying to preserve it
    if( H2O.OPT_ARGS.keepice == null )  cleanIce(iceRoot);
    else initializeFilesFromFolder(iceRoot);
  }

  // Clear the ICE directory
  public static void cleanIce(File dir) {
    for( File f : dir.listFiles() ) {
      if( f.isDirectory() ) cleanIce(f);
      f.delete();
    }
  }

  // Initializes Key/Value pairs for files on the local disk.
  private static void initializeFilesFromFolder(File dir) {
    for (File f : dir.listFiles()) {
      if( f.isDirectory() ) {
        initializeFilesFromFolder(f); // Recursively keep loading K/V pairs
      } else {
        Key k = decodeKey(f);
        Value ice = new Value(k,(int)f.length());
        ice.setdsk();
        H2O.putIfAbsent_raw(k,ice);
      }
    }
  }

  public static FileWriter logFile() {
    try { return new FileWriter(iceRoot+"/h2o.log"); }
    catch( IOException ioe ) { return null; }
  }

  // file implementation -------------------------------------------------------

  // the filename can be either byte encoded if it starts with % followed by
  // a number, or is a normal key name with special characters encoded in
  // special ways.
  // It is questionable whether we need this because the only keys we have on
  // ice are likely to be arraylet chunks

  // Verify bijection of key/file-name mappings.
  private static String key2Str(Key k, byte type) {
    String s = key2Str_impl(k,type);
    Key x;
    assert (x=str2Key_impl(s)).equals(k) : "bijection fail "+k+"."+(char)type+" <-> "+s+" <-> "+x;
    return s;
  }
  // Verify bijection of key/file-name mappings.
  private static Key str2Key(String s) {
    Key k = str2Key_impl(s);
    assert key2Str_impl(k,decodeType(s)).equals(s) : "bijection fail "+s+" <-> "+k;
    return k;
  }
  private static byte decodeType(String s) {
    String ext = s.substring(s.lastIndexOf('.')+1);
    return (byte)ext.charAt(0);
  }

  // Convert a Key to a suitable filename string
  private static String key2Str_impl(Key k, byte type) {
    // check if we are system key
    StringBuilder sb = new StringBuilder(k._kb.length/2+4);
    int i=0;
    if( k._kb[0]<32 ) {
      // System keys: hexalate all the leading non-ascii bytes
      sb.append('%');
      int j=k._kb.length-1;     // Backwards scan for 1st non-ascii
      while( j >= 0 && k._kb[j] >= 32 && k._kb[j] < 128 ) j--;
      for( ; i<=j; i++ ) {
        byte b = k._kb[i];
        int nib0 = ((b>>>4)&15)+'0';
        if( nib0 > '9' ) nib0 += 'A'-10-'0';
        int nib1 = ((b>>>0)&15)+'0';
        if( nib1 > '9' ) nib1 += 'A'-10-'0';
        sb.append((char)nib0).append((char)nib1);
      }
      sb.append('%');
    }
    // Escape the special bytes from 'i' to the end
    return escapeBytes(k._kb,i,sb).append('.').append((char)type).toString();
  }

  private static StringBuilder escapeBytes(byte[] bytes, int i, StringBuilder sb) {
    for( ; i<bytes.length; i++ ) {
      byte b = bytes[i];
      switch( b ) {
      case '%':  sb.append("%%"); break;
      case '.':  sb.append("%d"); break; // dot
      case '/':  sb.append("%s"); break; // slash
      case ':':  sb.append("%c"); break; // colon
      case '\\': sb.append("%b"); break; // backslash
      case '"':  sb.append("%q"); break; // quote
      case '\0': sb.append("%z"); break; // nullbit
      default:   sb.append((char)b); break;
      }
    }
    return sb;
  }

  // Convert a filename string to a Key
  private static Key str2Key_impl(String s) {
    String key = s.substring(0,s.lastIndexOf('.')); // Drop extension
    byte[] kb = new byte[(key.length()-1)/2];
    int i = 0, j = 0;
    if( (key.length()>2) && (key.charAt(0)=='%') && (key.charAt(1)>='0') && (key.charAt(1)<='9') ) {
      // Dehexalate until '%'
      for( i = 1; i < key.length(); i+=2 ) {
        if( key.charAt(i)=='%' ) break;
        char b0 = (char)(key.charAt(i+0)-'0');
        if( b0 > 9 ) b0 += '0'+10-'A';
        char b1 = (char)(key.charAt(i+1)-'0');
        if( b1 > 9 ) b1 += '0'+10-'A';
        kb[j++] = (byte)((b0<<4)|b1);  // De-hexelated byte
      }
      i++;                      // Skip the trailing '%'
    }
    // a normal key - ASCII with special characters encoded after % sign
    for( ; i < key.length(); ++i ) {
      byte b = (byte)key.charAt(i);
      if( b == '%' ) {
        switch( key.charAt(++i) ) {
        case '%':  b = '%' ; break;
        case 'b':  b = '\\'; break;
        case 'c':  b = ':' ; break;
        case 'd':  b = '.' ; break;
        case 'q':  b = '"' ; break;
        case 's':  b = '/' ; break;
        case 'z':  b = '\0'; break;
        default:   System.err.println("Invalid format of filename " + s + " at index " + i);
        }
      }
      if( j>=kb.length ) kb = Arrays.copyOf(kb,Math.max(2,j*2));
      kb[j++] = b;
    }
    // now in kb we have the key name
    return Key.make(Arrays.copyOf(kb,j));
  }

  private static final Key decodeKey(File f) {
    return str2Key(f.getName());
  }

  private static File encodeKeyToFile(Value v) {
    return encodeKeyToFile(v._key,(byte)(v.isArray()?'A':'V'));
  }
  private static File encodeKeyToFile(Key k, byte type) {
    return new File(iceRoot,getDirectoryForKey(k)+File.separator+key2Str(k,type));
  }

  private static String getDirectoryForKey(Key key) {
    if( key._kb[0] != Key.ARRAYLET_CHUNK )
      return "not_an_arraylet";
    // Reverse arraylet key generation
    byte[] b = ValueArray.getArrayKeyBytes(key);
    return escapeBytes(b,0,new StringBuilder(b.length)).toString();
  }

  // Read up to 'len' bytes of Value.  Value should already be persisted to
  // disk.  A racing delete can trigger a failure where we get a null return,
  // but no crash (although one could argue that a racing load&delete is a bug
  // no matter what).
  static byte[] fileLoad(Value v) {
    File f = encodeKeyToFile(v);
    if( f.length() < v._max ) { // Should be fully on disk... or
      assert !v.isPersisted() : f.length() + " " + v._max + " " + v._key;  // or it's a racey delete of a spilled value
      return null;              // No value
    }
    try {
      FileInputStream s = new FileInputStream(f);
      try {
        AutoBuffer ab = new AutoBuffer(s.getChannel(),true,Value.ICE);
        byte[] b = ab.getA1(v._max);
        ab.close();
        return b;
      } finally {
        s.close();
      }
    } catch( IOException e ) {  // Broken disk / short-file???
      throw new RuntimeException("File load failed: "+e);
    }
  }

  public static class PersistIceException extends Exception {}

  // Store Value v to disk.
  static void fileStore(Value v) throws IOException {
    // A perhaps useless cutout: the upper layers should test this first.
    if( v.isPersisted() ) return;
    new File(iceRoot,getDirectoryForKey(v._key)).mkdirs();
    // Nuke any prior file.
    FileOutputStream s = null;
    try {
      s = new FileOutputStream(encodeKeyToFile(v));
    } catch (FileNotFoundException e) {
      System.err.println("Encoding a key to a file failed!");
      System.err.println("Key: "+v._key.toString());
      System.err.println("Encoded: "+encodeKeyToFile(v));
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    try {
      byte[] m = v.memOrLoad(); // we are not single threaded anymore
      assert m != null && m.length == v._max; // Assert not saving partial files
      new AutoBuffer(s.getChannel(),false,Value.ICE).putA1(m,m.length).close();
      v.setdsk();             // Set as write-complete to disk
    } finally {
      if( s != null ) s.close();
    }
  }

  static void fileDelete(Value v) {
    assert !v.isPersisted();   // Upper layers already cleared out
    File f = encodeKeyToFile(v);
    f.delete();
    if( v.isArray() ) { // Also nuke directory if the top-level ValueArray dies
      f = new File(iceRoot,getDirectoryForKey(v._key));
      f.delete();
    }
  }

  static Value lazyArrayChunk( Key key ) {
    assert key._kb[0] == Key.ARRAYLET_CHUNK;
    assert key.home();          // Only do this on the home node
    File f = encodeKeyToFile(key,(byte)'V'/*typed as a Value chunk, not the array header*/);
    if( !f.isFile() ) return null;
    Value val = new Value(key,(int)f.length());
    val.setdsk();               // But its already on disk.
    return val;
  }
}
