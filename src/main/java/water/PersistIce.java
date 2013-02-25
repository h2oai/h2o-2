package water;

import java.io.*;

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
        Value ice = new Value(k,(int)f.length(), Value.ICE);
        ice.setdsk();
        H2O.putIfAbsent_raw(k,ice);
      }
    }
  }

  // file implementation -------------------------------------------------------

  // the filename can be either byte encoded if it starts with % followed by
  // a number, or is a normal key name with special characters encoded in
  // special ways.
  // It is questionable whether we need this because the only keys we have on
  // ice are likely to be arraylet chunks
  private static final Key decodeKey(File f) {
    String key = f.getName();
    key = key.substring(0,key.lastIndexOf('.'));
    byte[] kb = null;
    // a normal key - ASCII with special characters encoded after % sign
    if ((key.length()<=2) || (key.charAt(0)!='%') || (key.charAt(1)<'0') || (key.charAt(1)>'9')) {
      byte[] nkb = new byte[key.length()];
      int j = 0;
      for( int i = 0; i < key.length(); ++i ) {
        byte b = (byte)key.charAt(i);
        if( b == '%' ) {
          switch( key.charAt(++i) ) {
          case '%':  b = '%' ; break;
          case 'b':  b = '\\'; break;
          case 'c':  b = ':' ; break;
          case 'd':  b = '.' ; break;
          case 'q':  b = '"' ; break;
          case 's':  b = '/' ; break;
          case 'z':  b = '\0' ; break;
          default:   System.err.println("Invalid format of filename " + f.getName() + " at index " + i);
          }
        }
        nkb[j++] = b;
        kb = new byte[j];
        System.arraycopy(nkb,0,kb,0,j);
      }
    } else {
      // system key, encoded by % and then 2 bytes for each byte of the key
      kb = new byte[(key.length()-1)/2];
      int j = 0;
      // Then hexelate the entire thing
      for( int i = 1; i < key.length(); i+=2 ) {
        char b0 = (char)(key.charAt(i+0)-'0');
        if( b0 > 9 ) b0 += '0'+10-'A';
        char b1 = (char)(key.charAt(i+1)-'0');
        if( b1 > 9 ) b1 += '0'+10-'A';
        kb[j++] = (byte)((b0<<4)|b1);  // De-hexelated byte
      }
    }
    // now in kb we have the key name
    return Key.make(kb,decodeReplication(f));
  }

  private static byte decodeReplication(File f) {
    String ext = f.getName();
    ext = ext.substring(ext.lastIndexOf('.')+1);
    try {
      return (byte)Integer.parseInt(ext.substring(1));
    } catch (NumberFormatException e) {
      Log.die("[ice] Unable to decode filename "+f.getAbsolutePath());
      return 0;
    }
  }

  private static byte decodeType(File f) {
    String ext = f.getName();
    ext = ext.substring(ext.lastIndexOf('.')+1);
    return (byte)ext.charAt(0);
  }

  private static File encodeKeyToFile(Value v) {
    return encodeKeyToFile(v._key,(byte)(v._isArray!=0?'A':'V'));
  }
  private static File encodeKeyToFile(Key k, byte type) {
    // check if we are system key
    StringBuilder sb = null;
    if (k._kb[0]<32) {
      sb = new StringBuilder(k._kb.length/2+4);
      sb.append('%');
      for( byte b : k._kb ) {
        int nib0 = ((b>>>4)&15)+'0';
        if( nib0 > '9' ) nib0 += 'A'-10-'0';
        int nib1 = ((b>>>0)&15)+'0';
        if( nib1 > '9' ) nib1 += 'A'-10-'0';
        sb.append((char)nib0).append((char)nib1);
      }
    // or a normal key
    } else {
      sb = escapeBytes(k._kb);
    }
    // append the value type and replication factor
    sb.append('.');
    sb.append((char)type);
    sb.append(k.desired());
    return new File(iceRoot,getDirectoryForKey(k)+File.separator+sb.toString());
  }

  private static StringBuilder escapeBytes(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length*2);
    for( byte b : bytes ) {
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

  private static String getDirectoryForKey(Key key) {
    if( key._kb[0] != Key.ARRAYLET_CHUNK )
      return "not_an_arraylet";
    // Reverse arraylet key generation
    byte[] b = ValueArray.getArrayKeyBytes(key);
    return escapeBytes(b).toString();
  }

  // Read up to 'len' bytes of Value.  Value should already be persisted to
  // disk.  A racing delete can trigger a failure where we get a null return,
  // but no crash (although one could argue that a racing load&delete is a bug
  // no matter what).
  static byte[] fileLoad(Value v) {
    File f = encodeKeyToFile(v);
    if( f.length() < v._max ) { // Should be fully on disk... or
      assert !v.isPersisted(); // or it's a racey delete of a spilled value
      return null;              // No value
    }
    byte[] b = MemoryManager.malloc1(v._max);
    try {
      DataInputStream s = new DataInputStream(new FileInputStream(f));
      try {
        s.readFully(b, 0, v._max);
        return b;
      } finally {
        s.close();
      }
    } catch( IOException e ) {  // Broken disk / short-file???
      throw new RuntimeException("File load failed: "+e);
    }
  }

  // Store Value v to disk.
  static void fileStore(Value v) {
    // A perhaps useless cutout: the upper layers should test this first.
    if( v.isPersisted() ) return;
    try {
      new File(iceRoot,getDirectoryForKey(v._key)).mkdirs();
      // Nuke any prior file.
      OutputStream s = null;
      try {
        s = new FileOutputStream(encodeKeyToFile(v));
      } catch (FileNotFoundException e) {
        System.err.println("Encoding a key to a file failed!");
        System.err.println("Key: "+v._key.toString());
        System.err.println("Encoded: "+encodeKeyToFile(v));
        e.printStackTrace();
      }
      try {
        byte[] m = v.mem(); // we are not single threaded anymore
        assert m != null && m.length == v._max; // Assert not saving partial files
        s.write(m);
        v.setdsk();             // Set as write-complete to disk
      } finally {
        s.close();
      }
    } catch( IOException e ) {
      e.printStackTrace();
      throw new RuntimeException("File store failed: "+e);
    }
  }

  static void fileDelete(Value v) {
    assert !v.isPersisted();   // Upper layers already cleared out
    File f = encodeKeyToFile(v);
    f.delete();
    if( v._isArray != 0 ) { // Also nuke directory if the top-level ValueArray dies
      f = new File(iceRoot,getDirectoryForKey(v._key));
      f.delete();
    }
  }

  static Value lazyArrayChunk( Key key ) {
    assert key._kb[0] == Key.ARRAYLET_CHUNK;
    assert key.home();          // Only do this on the home node
    File f = encodeKeyToFile(key,Value.ICE);
    if( !f.isFile() ) return null;
    Value val = new Value(key,(int)f.length());
    val.setdsk();               // But its already on disk.
    return val;
  }
}
