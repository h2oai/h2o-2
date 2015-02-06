package water;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;

import water.util.Log;
import water.util.Utils;


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

  public String loadContent(String fromFile) {
    BufferedReader reader = null;
    StringBuilder sb = new StringBuilder();
    try {
      InputStream is = getResource2(fromFile);
      reader = new BufferedReader(new InputStreamReader(is));
      CharStreams.copy(reader, sb);
    } catch( IOException e ){
      Log.err(e);
    } finally {
      Closeables.closeQuietly(reader);
    }
    return sb.toString();
  }
  private final String _jarPath;
  private final ZipFile _h2oJar;
  private File _parentDir;
  private Weaver _weaver;

  static {
    try { _init = new Boot(); }
    catch( Exception e ) { throw new RuntimeException(e); } // Do not attempt logging: no boot-loader
  }

  public boolean fromJar() { return _h2oJar != null; }
  public String jarPath() { return _jarPath; }
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

    super(Thread.currentThread().getContextClassLoader());
    final String ownJar = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
    Log.POST(2000, "ownJar is " + ownJar);
    ZipFile jar = null;

    // do nothing if not run from jar
    if( ownJar.endsWith(".jar") ) {
      Log.POST(2001, "");
      _jarPath = URLDecoder.decode(ownJar, "UTF-8");
    }
    else if ( ownJar.endsWith(".jar/") ) {
      Log.POST(2002, "");
      // Some hadoop versions (like Hortonworks) will unpack the jar
      // file on their own.
      String stem = "h2o.jar";
      File f = new File (ownJar + stem);
      if (f.exists()) {
        Log.POST(2003, "");
	_jarPath = URLDecoder.decode(ownJar + stem, "UTF-8");
      }
      else {
	_jarPath = null;
      }
    }
    else {
      _jarPath = null;
    }

    if (_jarPath == null) {
      Log.POST(2004, "");
      this._jarHash = new byte[16];
      Arrays.fill(this._jarHash, (byte)0xFF);
      _h2oJar = null;
    }
    else {
      Log.POST(2005, "");
      InputStream is = new FileInputStream(_jarPath);
      _jarHash = getMD5(is);
      is.close();
      _h2oJar = new ZipFile(_jarPath);
    }

    Log.POST(2010, "_h2oJar is null: " + ((_h2oJar == null) ? "true" : "false"));
  }

  public static void main(String[] args) throws Exception {  _init.boot(args); }
  // NOTE: This method cannot be run from jar
  public static void main(Class main, String[] args) throws Exception {
    String[] packageNamesToWeave = { main.getPackage().getName()} ;
    main(main, args, packageNamesToWeave);
  }
  // NOTE: This method cannot be run from jar
  public static void main(Class main, String[] args, String[] packageNamesToWeave) throws Exception{
    for (String packageName : packageNamesToWeave) {
      weavePackage(packageName);
    }
    ArrayList<String> l = new ArrayList<String>(Arrays.asList(args));
    l.add(0, "-mainClass");
    l.add(1, main.getName());
    _init.boot2(l.toArray(new String[0]));
  }
  public static void weavePackage(String name) {
    Weaver.registerPackage(name);
  }
  public static String[] wovenPackages() {
    return Weaver._packages;
  }

  private URLClassLoader _systemLoader;
  private Method _addUrl;

  public void boot( String[] args ) throws Exception {
    try {
      boot2(args);
    }
    catch (Exception e) {
      Log.POST(119, e);
      throw (e);
    }
  }

  /**
   * Shutdown hook to delete tmp directory on exit.
   * Intent is to delete the unpacked jar files, not the log files or ICE files.
   */
  class DeleteDirHandler extends Thread {
    final String _dir;
    DeleteDirHandler(String dir) { _dir=dir; }
    void delete(File f) throws IOException {
      if (f.isDirectory())
        for (File c : f.listFiles())
          delete(c);
      if (!f.delete())
        throw new FileNotFoundException("Failed to delete file: " + f);
    }

    @Override
    public void run() {
      try { delete (new File (_dir)); }
      catch (Exception e) { /* silent lossage because we tried but cannot help */ }
    }
  }

  public void boot2( String[] args ) throws Exception {
    // Catch some log setup stuff before anything else can happen.
    boolean help = false;
    boolean version = false;
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      Log.POST(110, arg == null ? "(arg is null)" : "arg is: " + arg);
      if (arg.equals("-h") || arg.equals("--h") || arg.equals("-help") || arg.equals ("--help")) {
        help = true;
      }
      if (arg.equals("-version") || arg.equals ("--version")) {
        version = true;
      }
    }

    if (help) {
      H2O.printHelp();
      H2O.exit (0);
    }

    if (version) {
      H2O.printAndLogVersion();
      H2O.exit (0);
    }

    _systemLoader = (URLClassLoader) getSystemClassLoader();
    _addUrl = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
    _addUrl.setAccessible(true);

    if( fromJar() ) {
      // Calculate directory name of where to unpack JAR file stuff.
      String tmproottmpdir;
      {
        // Get --ice_root.
        String ice_root;
        {
          ice_root = H2O.DEFAULT_ICE_ROOT();
          for( int i=0; i<args.length; i++ )
            if( args[i].startsWith("--ice_root=") ) ice_root = args[i].substring(11);
            else if( args[i].startsWith("-ice_root=") ) ice_root = args[i].substring(10);
            else if( (args[i].equals("--ice_root") || args[i].equals("-ice_root")) && (i < args.length-1) )
              ice_root = args[i+1];
        }

        // Make a tmp directory in ice_root.
        File tmproot = new File(ice_root);
        if( !tmproot.mkdirs() && !tmproot.isDirectory() ) throw new IOException("Unable to create ice root: " + tmproot.getAbsolutePath());

        long now;
        String randomChars;
        String pid;
        {
          now = System.currentTimeMillis();
          pid = "unknown";

          Random r = new Random();
          byte[] bytes = new byte[4];
          r.nextBytes(bytes);
          randomChars = String.format("%02x%02x%02x%02x", bytes[0], bytes[1], bytes[2], bytes[3]);

          try {
            String s = ManagementFactory.getRuntimeMXBean().getName();
            Pattern p = Pattern.compile("([0-9]*).*");
            Matcher m = p.matcher(s);
            boolean b = m.matches();
            if (b == true) {
              pid = m.group(1);
            }
          }
          catch (Exception xe) {}
        }

        tmproottmpdir = tmproot + File.separator + "h2o-temp-" + now + "-" + randomChars + "-" + pid;
      }

      File dir = new File (tmproottmpdir);
      if (dir.exists()) {
        if( !dir.delete() ) throw new IOException("Failed to remove tmp file: " + dir.getAbsolutePath());
      }
      if( !dir.mkdir() )  throw new IOException("Failed to create tmp dir: "  + dir.getAbsolutePath());

      // This causes the tmp JAR unpack dir to delete on exit.
      // It does not delete logs or ICE stuff.
      Runtime.getRuntime().addShutdownHook(new DeleteDirHandler(dir.toString()));

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
      addInternalJars("log4j");
      addInternalJars("joda");
      addInternalJars("json");
      addInternalJars("tachyon");
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
    Log.POST(20, "before (in run) mainClass invoke " + mainClazz.getName());

    Method main = null;
    try {
      // First look for 'userMain', so that user code only exposes one 'main'
      // method. Problem showed up on samples where users launched the wrong one.
      main = mainClazz.getMethod("userMain", String[].class);
    } catch(NoSuchMethodException ex) {}
    if (main == null) {
      main = mainClazz.getMethod("main", String[].class);
    }
    Log.POST(20, (main == null) ? "main is null" : "main is not null");
    try {
      main.invoke(null, (Object) args);
    }
    catch (Exception e) {
      Log.POST(20, "invoke got an exception");
      Log.POST(20, "");
      Log.POST(20, e);
      throw e;
    }
    Log.POST(20, "after (in run) mainClass invoke "+ mainClazz.getName());

    int index = Arrays.asList(args).indexOf("-runClass");
    if( index >= 0 && args.length > index + 1 ) {
      String className = args[index + 1];    // Swap out for requested main
      args = Arrays.copyOfRange(args, index + 2, args.length);
      Class clazz = _init.loadClass(className,true);
      Log.POST(21, "before (in run) runClass invoke " + clazz.getName() + " main");
      clazz.getMethod("main",String[].class).invoke(null,(Object)args);
      Log.POST(21, "after (in run) runClass invoke " + clazz.getName() + " main");
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
      Log.POST(22, "before (in addExternalJars) invoke _addUrl " + file.toURI().toURL());
      _addUrl.invoke(_systemLoader, file.toURI().toURL());
      Log.POST(22, "after (in addExternalJars) invoke _addUrl " + file.toURI().toURL());
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
      InputStream is = _systemLoader.getResourceAsStream("resources"+uri);
      if (is==null) is = this.getClass().getClassLoader().getResourceAsStream("resources"+uri);
      if (is==null) is = Thread.currentThread().getContextClassLoader().getResourceAsStream("resources"+uri);
      return is;
    } else {
      try {
        File resources  = new File("lib/resources");
        if(!resources.exists()) {
          // IDE mode assumes classes are in target/classes. Not using current path
          // to allow running from other locations.
          String h2oClasses = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
          resources = new File(h2oClasses + "/../../lib/resources");
        }
        return new FileInputStream(new File(resources, uri));
      } catch (FileNotFoundException e) {
        Log.err("Trying system loader because : ", e);
        return _systemLoader.getResourceAsStream("resources"+uri);
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
  @Override public synchronized Class loadClass( String name, boolean resolve ) throws ClassNotFoundException {
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
    if( z != null ) {
      // Occasionally it's useful to print out class names that are actually Weaved.
      // Leave this commented out println here so I can easily find it for next time.
      //   System.out.println("WEAVED: " + name);
      return z;
    }
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
      names.set(i, Utils.className(n));
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
