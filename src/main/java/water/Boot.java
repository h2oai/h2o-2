package water;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import water.util.*;
import water.util.Log.Tag.Sys;


/** Initializer class for H2O.
 *
 * Unpacks all the dependencies and H2O implementation from the jar file, sets
 * the loader to be able to load all the classes properly and then executes the
 * main method of the H2O class.
 *
 * Does nothing if the H2O is not run from a jar archive. (This *is* a feature,
 * at least for the time being so that we can continue using different IDEs).
 */
public class Boot extends ClassLoader {

  public static final Boot _init;
  public final byte[] _jarHash;

  private final ZipFile _h2oJar;
  private File _parentDir;
  private Weaver _weaver;

  static {
    try { _init = new Boot(); }
    catch( Exception e ) { throw new RuntimeException(e); } // Do not attempt logging: no boot-loader
  }

  public boolean fromJar() { return _h2oJar != null; }
  private byte[] getMD5(InputStream is) throws IOException {
    try {
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      byte[] buf = new byte[4096];
      int pos;
      while( (pos = is.read(buf)) > 0 ) md5.update(buf, 0, pos);
      return md5.digest();
    } catch( NoSuchAlgorithmException e ) {
      throw  Log.errRTExcept(e);
    } finally {
      Utils.close(is);
    }
  }

  private Boot() throws IOException {
    final String ownJar = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
    ZipFile jar = null;
    if( ownJar.endsWith(".jar") ) { // do nothing if not run from jar
      String path = URLDecoder.decode(ownJar, "UTF-8");
      InputStream is = new FileInputStream(path);
      this._jarHash = getMD5(is);
      is.close();

      jar = new ZipFile(path);
    } else {
      this._jarHash = new byte[16];
      Arrays.fill(this._jarHash, (byte)0xFF);
    }
    _h2oJar = jar;
  }

  public static void main(String[] args) throws Exception {  _init.boot(args); }

  private URLClassLoader _systemLoader;
  private Method _addUrl;

  private void boot( String[] args ) throws Exception {
    if( fromJar() ) {
      _systemLoader = (URLClassLoader)getSystemClassLoader();
      _addUrl = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
      _addUrl.setAccessible(true);

      // Find --ice_root and use it to set the unpack directory
      String sroot = System.getProperty("java.io.tmpdir");
      for( int i=0; i<args.length; i++ )
        if( args[i].startsWith("--ice_root=") || args[i].startsWith("-ice_root=") )
          sroot = args[i].substring(11);
        else if( (args[i].equals("--ice_root") || args[i].startsWith("-ice_root")) && i < args.length-1 )
          sroot = args[i+1];

      // Make a tmp directory in --ice_root (or java.io.tmpdir) to unpack into
      File tmproot = new File(sroot);
      if( !tmproot.mkdirs() && !tmproot.isDirectory() )  throw new IOException("Unable to create ice root: "  + tmproot.getAbsolutePath());

      File dir = File.createTempFile("h2o-temp-", "", tmproot);
      if( !dir.delete() ) throw new IOException("Failed to remove tmp file: " + dir.getAbsolutePath());
      if( !dir.mkdir() )  throw new IOException("Failed to create tmp dir: "  + dir.getAbsolutePath());
      dir.deleteOnExit();
      _parentDir = dir;         // Set a global instead of passing the dir about?
      Log.debug("Extracting jar into " + _parentDir);

      // Make all the embedded jars visible to the custom class loader
      extractInternalFiles(); // Resources
      addInternalJars("apache");
      addInternalJars("gson");
      addInternalJars("junit");
      addInternalJars("jama");
      addInternalJars("poi");
      addInternalJars("s3");
      addInternalJars("jets3t");
    }

    run(args);
  }

  public static void run(String[] args) throws Exception {
    // Figure out the correct main class to call
    String mainClass = "water.H2O";
    if(args != null) {
      int index = Arrays.asList(args).indexOf("-mainClass");
      if( index >= 0 && args.length > index + 1 ) {
        mainClass = args[index + 1];    // Swap out for requested main
        args = Arrays.copyOfRange(args, index + 2, args.length);
      }
    }
    Class mainClazz = _init.loadClass(mainClass,true);
    mainClazz.getMethod("main",String[].class).invoke(null,(Object)args);
    int index = Arrays.asList(args).indexOf("-runClass");
    if( index >= 0 && args.length > index + 1 ) {
      String className = args[index + 1];    // Swap out for requested main
      args = Arrays.copyOfRange(args, index + 2, args.length);
      Class clazz = _init.loadClass(className,true);
      clazz.getMethod("main",String[].class).invoke(null,(Object)args);
    }
  }

  /** Returns an external File for the internal file name. */
  public File internalFile(String name) { return new File(_parentDir, name); }

  /** Add a jar to the system classloader */
  public void addInternalJars(String name) throws IllegalAccessException, InvocationTargetException, MalformedURLException {
    addExternalJars(internalFile(name));
  }

  /** Adds all jars in given directory to the classpath. */
  public void addExternalJars(File file) throws IllegalAccessException, InvocationTargetException, MalformedURLException {
    assert file.exists() : "Unable to find external file: " + file.getAbsolutePath();
    if( file.isDirectory() ) {
      for( File f : file.listFiles() ) addExternalJars(f);
    } else if( file.getName().endsWith(".jar") ) {
      _addUrl.invoke(_systemLoader, file.toURI().toURL());
    }
  }

  /** Extracts the libraries from the jar file to given local path.   */
  private void extractInternalFiles() throws IOException {
    Enumeration entries = _h2oJar.entries();
    while( entries.hasMoreElements() ) {
      ZipEntry e = (ZipEntry) entries.nextElement();
      String name = e.getName();
      if( e.isDirectory() ) continue; // mkdirs() will handle these
      if(! name.endsWith(".jar") ) continue;

      // extract the entry
      File out = internalFile(name);
      out.getParentFile().mkdirs();
      try {
        FileOutputStream fos = new FileOutputStream(out);
        BufferedInputStream  is = new BufferedInputStream (_h2oJar.getInputStream(e));
        BufferedOutputStream os = new BufferedOutputStream(fos);
        int read;
        byte[] buffer = new byte[4096];
        while( (read = is.read(buffer)) != -1 ) os.write(buffer,0,read);
        os.flush();
        fos.getFD().sync();     // Force the output; throws SyncFailedException if full
        os.close();
        is.close();
      } catch( FileNotFoundException ex ) {
        // Expected FNF if 2 H2O instances are attempting to unpack in the same directory
      } catch( IOException ex ) {
        Log.die("Unable to extract file "+name+" because of "+ex+". Make sure that directory " + _parentDir + " contains at least 50MB of free space to unpack H2O libraries.");
        throw ex; // dead code
      }
    }
  }

  public InputStream getResource2(String uri) {
    if( fromJar() ) {
      return _systemLoader.getResourceAsStream("resources"+uri);
    } else { // to allow us to read things not only from the loader
      try {
        return new FileInputStream(new File("lib/resources"+uri));
      } catch (FileNotFoundException e) {
        Log.err(e);
        return null;
      }
    }
  }

  // --------------------------------------------------------------------------
  //
  // Auto-Serialization!
  //
  // At Class-load-time, insert serializers for all subclasses of Iced & DTask
  // that do not already contain serializers.  We are limited to serializing
  // primitives, arrays of primitivies, Keys, and Strings.
  //
  // --------------------------------------------------------------------------

  // Intercept class loads that would otherwise go to the parent loader
  // (probably the System loader) and try to auto-add e.g. serialization
  // methods to classes that inherit from DTask & Iced.  Notice that this
  // changes the default search order: existing classes first, then my class
  // search, THEN the System or parent loader.
  public synchronized Class loadClass( String name, boolean resolve ) throws ClassNotFoundException {
    assert !name.equals(Weaver.class.getName());
    Class z = loadClass2(name);      // Do all the work in here
    if( resolve ) resolveClass(z);   // Resolve here instead in the work method
    return z;
  }

  // Run the class lookups in my favorite non-default order.
  private final Class loadClass2( String name ) throws ClassNotFoundException {
    Class z = findLoadedClass(name); // Look for pre-existing class
    if( z != null ) return z;
    if( _weaver == null ) _weaver = new Weaver();
    z = _weaver.weaveAndLoad(name, this);    // Try the Happy Class Loader
    if( z != null ) return z;
    z = getParent().loadClass(name); // Try the parent loader.  Probably the System loader.
    if( z != null ) return z;
    return z;
  }

  // --------------------------------------------------------------------------
  //
  // Lists H2O classes
  //
  // --------------------------------------------------------------------------

  public static List<String> getClasses() {
    ArrayList<String> names = new ArrayList<String>();
    if(_init._h2oJar != null) {
      for( Enumeration<ZipEntry> e = (Enumeration) _init._h2oJar.entries(); e.hasMoreElements(); ) {
        String name = e.nextElement().getName();
        if( name.endsWith(".class") )
          names.add(name);
      }
    } else
      findClasses(new File(CLASSES), names);

    for( int i = 0; i < names.size(); i++ ) {
      String n = names.get(i);
      names.set(i, n.replace('\\', '/').replace('/', '.').substring(0, n.length() - 6));
    }
    return names;
  }
  private static final String CLASSES = "target/classes";
  private static void findClasses(File folder, ArrayList<String> names) {
    for( File file : folder.listFiles() ) {
      if( file.isDirectory() )
        findClasses(file, names);
      else if( file.getPath().endsWith(".class") )
        names.add(file.getPath().substring(CLASSES.length() + 1));
    }
  }

  // --------------------------------------------------------------------------
  // Some global static variables used to pass state between System threads and
  // H2O threads, such as the GC call-back thread and the MemoryManager threads.
  static public volatile long HEAP_USED_AT_LAST_GC;
  static public volatile long TIME_AT_LAST_GC=System.currentTimeMillis();
  static private final Object _store_cleaner_lock = new Object();
  static public void kick_store_cleaner() {
    synchronized(_store_cleaner_lock) { _store_cleaner_lock.notifyAll(); }
  }
  static public void block_store_cleaner() {
    synchronized( _store_cleaner_lock ) {
      try { _store_cleaner_lock.wait(5000); } catch (InterruptedException ie) { }
    }
  }
}
