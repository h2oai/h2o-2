package water.persist;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import water.*;
import water.api.Constants.Schemes;
import water.util.Log;

public abstract class Persist<T> {
  // All available back-ends, C.f. Value for indexes
  public static final Persist[] I = new Persist[8];
  public static final long UNKNOWN = 0;

  public static void initialize() {}

  static {
    Log.POST(3100);
    Persist ice = null;
    URI uri = H2O.ICE_ROOT;
    if( uri != null ) { // Otherwise class loaded for reflection
      boolean windowsPath = uri.toString().matches("^[a-zA-Z]:.*");

      Log.POST(3101, "uri getPath(): " + uri.getPath());
      Log.POST(3101, "windowsPath: " + (windowsPath ? "true" : "false"));
      if ( windowsPath ) {
        ice = new PersistFS(new File(uri.toString()));
      }
      else if ((uri.getScheme() == null) || Schemes.FILE.equals(uri.getScheme())) {
        ice = new PersistFS(new File(uri.getPath()));
      }
      else if( Schemes.HDFS.equals(uri.getScheme()) ) {
        ice = new PersistHdfs(uri);
      }

      // System.out.println("TOM ice is null: " + ((ice == null) ? "true" : "false"));

// TODO ice on other back-ends?
//        else if( Schemes.S3.equals(uri.getScheme()) ) {
//          ice = new PersistS3(uri);
//        } else if( Schemes.NFS.equals(uri.getScheme()) ) {
//          ice = new PersistNFS(uri);
//        }
      I[Value.ICE    ] = ice;
      Log.POST(3102, "");
      try {
        I[Value.HDFS] = new PersistHdfs();
      }
      catch (Throwable e) {
        Log.POST(3103, e.toString());
        e.printStackTrace();
      }
      Log.POST(3104, "");
      I[Value.S3     ] = new PersistS3();
      I[Value.NFS    ] = new PersistNFS();
      I[Value.TACHYON] = new PersistTachyon();

      // By popular demand, clear out ICE on startup instead of trying to preserve it
      if( H2O.OPT_ARGS.keepice == null ) {
        final Persist ice2 = ice;
        new Thread() {
          public void run() {
            ice2.clear();
          }
        }.start();
      }
      else {
        ice.loadExisting();
      }
      Log.POST(3105, "");
    }
  }

  public static Persist getIce() {
    return I[Value.ICE];
  }

  public abstract String getPath();

  public abstract void clear();

  /**
   * Load all Key/Value pairs that can be found on the backend.
   */
  public abstract void loadExisting();

  /**
   * Value should already be persisted to disk. A racing delete can trigger a failure where we get a
   * null return, but no crash (although one could argue that a racing load and delete is a bug no
   * matter what).
   */
  public abstract byte[] load(Value v);

  public abstract void store(Value v);

  public abstract void delete(Value v);

  public long getUsableSpace() {
    return UNKNOWN;
  }

  public long getTotalSpace() {
    return UNKNOWN;
  }

  //the filename can be either byte encoded if it starts with % followed by
  // a number, or is a normal key name with special characters encoded in
  // special ways.
  // It is questionable whether we need this because the only keys we have on
  // ice are likely to be arraylet chunks

  static String getIceName(Value v) {
    return getIceName(v._key, (byte) 'V');
  }

  static String getIceName(Key k, byte type) {
    return getIceDirectory(k) + File.separator + key2Str(k, type);
  }

  static String getIceDirectory(Key key) {
    return "not_an_arraylet";
  }

  // Verify bijection of key/file-name mappings.
  private static String key2Str(Key k, byte type) {
    String s = key2Str_impl(k, type);
    Key x;
    assert (x = str2Key_impl(s)).equals(k) : "bijection fail " + k + "." + (char) type + " <-> " + s + " <-> " + x;
    return s;
  }

  // Verify bijection of key/file-name mappings.
  static Key str2Key(String s) {
    Key k = str2Key_impl(s);
    assert key2Str_impl(k, decodeType(s)).equals(s) : "bijection fail " + s + " <-> " + k;
    return k;
  }

  private static byte decodeType(String s) {
    String ext = s.substring(s.lastIndexOf('.') + 1);
    return (byte) ext.charAt(0);
  }

  // Convert a Key to a suitable filename string
  private static String key2Str_impl(Key k, byte type) {
    // check if we are system key
    StringBuilder sb = new StringBuilder(k._kb.length / 2 + 4);
    int i = 0;
    if( k._kb[0] < 32 ) {
      // System keys: hexalate all the leading non-ascii bytes
      sb.append('%');
      int j = k._kb.length - 1;     // Backwards scan for 1st non-ascii
      while( j >= 0 && k._kb[j] >= 32 && k._kb[j] < 128 )
        j--;
      for( ; i <= j; i++ ) {
        byte b = k._kb[i];
        int nib0 = ((b >>> 4) & 15) + '0';
        if( nib0 > '9' ) nib0 += 'A' - 10 - '0';
        int nib1 = ((b >>> 0) & 15) + '0';
        if( nib1 > '9' ) nib1 += 'A' - 10 - '0';
        sb.append((char) nib0).append((char) nib1);
      }
      sb.append('%');
    }
    // Escape the special bytes from 'i' to the end
    return escapeBytes(k._kb, i, sb).append('.').append((char) type).toString();
  }

  private static StringBuilder escapeBytes(byte[] bytes, int i, StringBuilder sb) {
    for( ; i < bytes.length; i++ ) {
      char b = (char)bytes[i], c=0;
      switch( b ) {
      case '%': c='%'; break;
      case '.': c='d'; break;
      case '/': c='s'; break;
      case ':': c='c'; break;
      case '"': c='q'; break;
      case '>': c='g'; break;
      case '\\':c='b'; break;
      case '\0':c='z'; break;
      }
      if( c!=0 ) sb.append('%').append(c);
      else sb.append(b);
    }
    return sb;
  }

  // Convert a filename string to a Key
  private static Key str2Key_impl(String s) {
    String key = s.substring(0, s.lastIndexOf('.')); // Drop extension
    byte[] kb = new byte[(key.length() - 1) / 2];
    int i = 0, j = 0;
    if( (key.length() > 2) && (key.charAt(0) == '%') && (key.charAt(1) >= '0') && (key.charAt(1) <= '9') ) {
      // Dehexalate until '%'
      for( i = 1; i < key.length(); i += 2 ) {
        if( key.charAt(i) == '%' ) break;
        char b0 = (char) (key.charAt(i + 0) - '0');
        if( b0 > 9 ) b0 += '0' + 10 - 'A';
        char b1 = (char) (key.charAt(i + 1) - '0');
        if( b1 > 9 ) b1 += '0' + 10 - 'A';
        kb[j++] = (byte) ((b0 << 4) | b1);  // De-hexelated byte
      }
      i++;                      // Skip the trailing '%'
    }
    // a normal key - ASCII with special characters encoded after % sign
    for( ; i < key.length(); ++i ) {
      byte b = (byte) key.charAt(i);
      if( b == '%' ) {
        switch( key.charAt(++i) ) {
        case '%':  b = '%';  break;
        case 'c':  b = ':';  break;
        case 'd':  b = '.';  break;
        case 'g':  b = '>';  break;
        case 'q':  b = '"';  break;
        case 's':  b = '/';  break;
        case 'b':  b = '\\'; break;
        case 'z':  b = '\0'; break;
        default:
          Log.warn("Invalid format of filename " + s + " at index " + i);
        }
      }
      if( j >= kb.length ) kb = Arrays.copyOf(kb, Math.max(2, j * 2));
      kb[j++] = b;
    }
    // now in kb we have the key name
    return Key.make(Arrays.copyOf(kb, j));
  }

  /** Return default URI of server to fetch data */
  public String getDefaultURI() { return null; }
  /** Create a client to communicate with default URI server */
  public final T createClient() throws IOException { return createClient(getDefaultURI()); }
  /** Create a client for given URI. */
  public T createClient(String uri) throws IOException { throw H2O.unimpl(); }
}
