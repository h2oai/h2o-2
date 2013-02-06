package H2OInit;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javassist.*;


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
  private final File _parentDir;
  File _binlib;

  // javassist support for rewriting class files
  private ClassPool _pool; // The pool of altered classes
  private CtClass[] _serBases;
  private CtClass _enum;

  // junk enum to force early loading for javaassist
  private static enum x { x }; @SuppressWarnings("unused") private static x _x;

  static {
    try {
      _init = new Boot();
    } catch( Exception e ) {
      throw new Error(e);
    }
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
      throw new RuntimeException(e);
    } finally {
      try { is.close(); } catch( IOException e ) { }
    }
  }

  private Boot() throws IOException {
    final String ownJar = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
    ZipFile jar = null;
    File dir = null;
    if( ownJar.endsWith(".jar") ) { // do nothing if not run from jar
      String path = URLDecoder.decode(ownJar, "UTF-8");
      InputStream is = new FileInputStream(path);
      this._jarHash = getMD5(is);
      is.close();

      jar = new ZipFile(path);
      dir = File.createTempFile("h2o-temp-", "");
      if( !dir.delete() ) throw new IOException("Failed to remove tmp file: " + dir.getAbsolutePath());
      if( !dir.mkdir() )  throw new IOException("Failed to create tmp dir: "  + dir.getAbsolutePath());
      dir.deleteOnExit();
    } else {
      this._jarHash = new byte[16];
      Arrays.fill(this._jarHash, (byte)0xFF);
    }
    _h2oJar = jar;
    _parentDir = (dir==null) ? new File(".") : dir;
    // javassist support for rewriting class files
    _pool = ClassPool.getDefault();
  }

  public static void main(String[] args) throws Exception {  _init.boot(args); }

  private URLClassLoader _systemLoader;
  private Method _addUrl;

  private void boot( String[] args ) throws Exception {
    if( fromJar() ) {
      _systemLoader = (URLClassLoader)getSystemClassLoader();
      _addUrl = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
      _addUrl.setAccessible(true);

      // Make all the embedded jars visible to the custom class loader
      extractInternalFiles(); // Extract e.g. SIGAR's .dll & .so files
      File binlib = internalFile("binlib");
      System.setProperty("org.hyperic.sigar.path", binlib.getAbsolutePath());
      addInternalJars("sigar");
      addInternalJars("apache");
      addInternalJars("gson");
      addInternalJars("junit");
      addInternalJars("jama");
      addInternalJars("poi");
      addInternalJars("s3");
    } else {
      System.setProperty("org.hyperic.sigar.path", "lib/binlib");
    }

    // Figure out the correct main class to call
    String mainClass = "water.H2O"; // Default mainClass
    if( args.length >= 2 && args[0].equals("-mainClass") ) {
      mainClass = args[1];    // Swap out for requested main
      args = Arrays.copyOfRange(args, 2, args.length);
    }

    // Call "main"!
    Class h2oclazz = loadClass(mainClass,true);
    h2oclazz.getMethod("main",String[].class).invoke(null,(Object)args);
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
  private void extractInternalFiles() {
    Enumeration entries = _h2oJar.entries();
    while( entries.hasMoreElements() ) {
      ZipEntry e = (ZipEntry) entries.nextElement();
      String name = e.getName();
      File out = internalFile(name);
      out.getParentFile().mkdirs();
      if( e.isDirectory() ) continue; // mkdirs() will handle these

      // extract the entry
      try {
        BufferedInputStream  is = new BufferedInputStream (_h2oJar.getInputStream(e));
        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(out));
        int read;
        byte[] buffer = new byte[4096];
        while( (read = is.read(buffer)) != -1 ) os.write(buffer,0,read);
        os.flush();
        os.close();
        is.close();
      } catch( FileNotFoundException ex ) {
        // Expected FNF if 2 H2O instances are attempting to unpack in the same directory
      } catch( IOException ex ) {
        System.err.println("Unable to extract file "+name+" because "+ex);
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
    Class z = loadClass2(name,resolve);      // Do all the work in here
    if( resolve ) resolveClass(z);           // Resolve here instead in the work method
    return z;
  }

  // Run the class lookups in my favorite non-default order.
  private final Class loadClass2( String name, boolean resolve ) throws ClassNotFoundException {
    Class z = findLoadedClass(name); // Look for pre-existing class
    if( z != null ) return z;
    z = javassistLoadClass(name);    // Try the Happy Class Loader
    if( z != null ) return z;
    z = getParent().loadClass(name); // Try the parent loader.  Probably the System loader.
    if( z != null ) return z;
    return z;
  }

  // See if javaassist can find this class; if so then check to see if it is a
  // subclass of water.DTask, and if so - alter the class before returning it.
  public synchronized Class javassistLoadClass( String name ) {
    try {
      CtClass cc = _pool.get(name); // Full Name Lookup
      if( cc == null ) return null; // Oops?  Try the system loader, but expected to work
      String pack = cc.getPackageName();
      if( !pack.startsWith("water") &&
          !pack.startsWith("hex") &&
          !pack.startsWith("test") &&
          !pack.startsWith("org.junit") &&
          true ) return null; // Not in my package

      // We need the base classes before we can ask "subclassOf"
      if( _serBases == null ) {
        _serBases = new CtClass[] {
            _pool.get("water.Iced"),
            _pool.get("water.DTask"),
        };
        for( CtClass c : _serBases ) c.toClass(this, null);
      }
      for( CtClass base : _serBases )
        if( cc != base && cc.subclassOf(base) )
          return javassistLoadClass(cc);
      if( _enum == null ) _enum = _pool.get("java.lang.Enum");
      if( cc != _enum && cc.subclassOf(_enum) )
        return javassistLoadClass(cc);
      return cc.toClass(this, null);
    } catch( NotFoundException nfe ) {
      return null;              // Not found?  Use the normal loader then
    } catch( CannotCompileException cce ) { // Expected to compile
      throw new RuntimeException(cce);
    }
  }

  public synchronized Class javassistLoadClass( CtClass cc ) throws NotFoundException, CannotCompileException {
    // Serialize parent class first
    CtClass scc = cc.getSuperclass(); // See if the super is already done
    if( !scc.isFrozen() && scc != _enum ) // Super not done?
      javassistLoadClass(scc);        // Recursively serialize

    // Serialize enums first, since we need the raw_enum function for this class
    for( CtField ctf : cc.getDeclaredFields() ) {
      CtClass base = ctf.getType();
      if( base != _enum && base != cc && !base.isFrozen() && base.subclassOf(_enum) )
        javassistLoadClass(base); // Recursively serialize
    }
    return addSerializationMethods(cc);
  }

  // Returns true if this method pre-exists *in the local class*.
  // Returns false otherwise, which requires a local method to be injected
  private static boolean hasExisting( String methname, String methsig, CtBehavior ccms[] ) throws NotFoundException {
    for( CtBehavior cm : ccms )
      if( cm.getName     ().equals(methname) &&
          cm.getSignature().equals(methsig ) )
        return true;
    return false;
  }

  // This method is handed a CtClass which is known to be a subclass of
  // water.DTask.  Add any missing serialization methods.
  Class addSerializationMethods( CtClass cc ) throws CannotCompileException, NotFoundException {
    if( cc.subclassOf(_enum) ) {
      exposeRawEnumArray(cc);
    } else {
      cc.setModifiers(javassist.Modifier.setPublic(cc.getModifiers()));
      ensureSerMethods(cc);
      ensureNullaryCtor(cc);
      ensureNewInstance(cc);
    }
    return cc.toClass(this, null);
  }

  private void exposeRawEnumArray(CtClass cc) throws NotFoundException, CannotCompileException {
      CtField field;
      try {
        field = cc.getField("$VALUES");
      } catch( NotFoundException nfe ) {
        // Eclipse apparently stores this in a different place.
        field = cc.getField("ENUM$VALUES");
      }
      String body = "static "+cc.getName()+" raw_enum(int i) { return i==255?null:"+field.getName()+"[i]; } ";
      try {
        cc.addMethod(CtNewMethod.make(body,cc));
      } catch( CannotCompileException ce ) {
        System.out.println("--- Compilation failure while compiler raw_enum for "+cc.getName());
        System.out.println(body);
        System.out.println("------");
        throw ce;
      }
  }

  private void ensureNewInstance(CtClass cc) throws NotFoundException, CannotCompileException {
    CtMethod ccms[] = cc.getDeclaredMethods();
    if( !hasExisting("newInstance", "()Lwater/Freezable;", ccms) ) {
      cc.addMethod(CtNewMethod.make(
          "public water.Freezable newInstance() {\n" +
          "    return new " +cc.getName()+"();\n" +
          "}", cc));
    }
  }

  private void ensureNullaryCtor(CtClass cc) throws NotFoundException, CannotCompileException {
    // Build a null-ary constructor if needed
    String clzname = cc.getSimpleName();
    if( !hasExisting(clzname,"()V",cc.getDeclaredConstructors()) ) {
      String body = "public "+clzname+"() { }";
      cc.addConstructor(CtNewConstructor.make(body,cc));
    } else {
      CtConstructor ctor = cc.getConstructor("()V");
      ctor.setModifiers(javassist.Modifier.setPublic(ctor.getModifiers()));
    }
  }

  private void ensureSerMethods(CtClass cc) throws NotFoundException, CannotCompileException {
    // Check for having "read" and "write".  Either All or None of read & write
    // must be defined.  Note that I use getDeclaredMethods which returns only
    // the local methods.  The singular getDeclaredMethod searches for a
    // specific method *up into superclasses*, which will trigger premature
    // loading of those superclasses.
    CtMethod ccms[] = cc.getDeclaredMethods();
    boolean w = hasExisting("write", "(Lwater/AutoBuffer;)Lwater/AutoBuffer;", ccms);
    boolean r = hasExisting("read", "(Lwater/AutoBuffer;)Lwater/Freezable;", ccms);
    if( w && r ) return;
    if( w || r )
      throw new Error(cc.getName() +" must implement both " +
      		"read(AutoBuffer) and write(AutoBuffer) or neither");

    // Add the serialization methods: read, write.
    CtField ctfs[] = cc.getDeclaredFields();

    // We cannot call Iced.xxx, as these methods always throw a
    // RuntimeException (to make sure we noisely fail instead of silently
    // fail).  But we DO need to call the super-chain of serialization methods
    // - except for DTask.
    boolean callsuper = true;
    for( CtClass base : _serBases )
      if( cc.getSuperclass() == base ) callsuper = false;

    // Running example is:
    //   class Crunk extends DTask {
    //     int _x;  int _xs[];  double _d;
    //   }

    // Build a write method that looks something like this:
    //     public AutoBuffer write( AutoBuffer s ) {
    //       s.put4(_x);
    //       s.putA4(_xs);
    //       s.put8d(_d);
    //     }
    make_body(cc,ctfs,callsuper,
              "public water.AutoBuffer write(water.AutoBuffer ab) {\n",
              "  super.write(ab);\n",
              "  ab.put%z(%s);\n",
              "  ab.putEnum(%s);\n",
              "  ab.put%z(%s);\n",
              "  return ab;\n" +
              "}");

    // Build a read method that looks something like this:
    //     public T read( AutoBuffer s ) {
    //       _x = s.get4();
    //       _xs = s.getA4();
    //       _d = s.get8d();
    //     }
    make_body(cc,ctfs,callsuper,
              "public water.Freezable read(water.AutoBuffer s) {\n",
              "  super.read(s);\n",
              "  %s = s.get%z();\n",
              "  %s = %c.raw_enum(s.get1());\n",
              "  %s = (%C)s.get%z(%c.class);\n",
              "  return this;\n" +
              "}");
  }

  // Produce a code body with all these fill-ins.
  private final void make_body(CtClass cc, CtField[] ctfs, boolean callsuper,
                               String header,
                               String supers,
                               String prims,
                               String enums,
                               String freezables,
                               String trailer
                               ) throws CannotCompileException, NotFoundException {
    StringBuilder sb = new StringBuilder();
    sb.append(header);
    if( callsuper ) sb.append(supers);
    boolean debug_print = false;
    for( CtField ctf : ctfs ) {
      int mods = ctf.getModifiers();
      if( javassist.Modifier.isTransient(mods) || javassist.Modifier.isStatic(mods) ) {
        debug_print |= ctf.getName().equals("DEBUG_WEAVER");
        continue;  // Only serialize not-transient instance fields (not static)
      }
      CtClass base = ctf.getType();
      while( base.isArray() ) base = base.getComponentType();

      int ftype = ftype(cc, ctf.getSignature() );   // Field type encoding
      if( ftype%20 == 9 ) {
        sb.append(freezables);
      } else if( ftype%20 == 10 ) { // Enums
        sb.append(enums);
      } else {
        sb.append(prims);
      }

      String z = FLDSZ1[ftype % 20];
      for(int i = 0; i < ftype / 20; ++i ) z = 'A'+z;
      subsub(sb, "%z", z);                                         // %z ==> short type name
      subsub(sb, "%s", ctf.getName());                             // %s ==> field name
      subsub(sb, "%c", base.getName().replace('$', '.'));          // %c ==> base class name
      subsub(sb, "%C", ctf.getType().getName().replace('$', '.')); // %C ==> full class name

    }
    sb.append(trailer);
    String body = sb.toString();
    if( debug_print ) {
      System.out.println(cc.getName()+" "+body);
    }

    try {
      cc.addMethod(CtNewMethod.make(body,cc));
    } catch( CannotCompileException ce ) {
      System.out.println("--- Compilation failure while compiler serializers for "+cc.getName());
      System.out.println(body);
      System.out.println("------");
      throw ce;
    }
  }

  static private final String[] FLDSZ1 = {
    "Z","1","2","2","4","4f","8","8d","Str","","Enum" // prims, String, Freezable, Enum
  };

  // Field types:
  // 0-7: primitives
  // 8,9, 10: String, Freezable, Enum
  // 20-27: array-of-prim
  // 28,29, 30: array-of-String, Freezable, Enum
  // Barfs on all others (eg Values or array-of-Frob, etc)
  private int ftype( CtClass ct, String sig ) throws NotFoundException {
    switch( sig.charAt(0) ) {
    case 'Z': return 0;         // Booleans: I could compress these more
    case 'B': return 1;         // Primitives
    case 'C': return 2;
    case 'S': return 3;
    case 'I': return 4;
    case 'F': return 5;
    case 'J': return 6;
    case 'D': return 7;
    case 'L':                   // Handled classes
      if( sig.equals("Ljava/lang/String;") ) return 8;

      String clz = sig.substring(1,sig.length()-1).replace('/', '.');
      CtClass argClass = _pool.get(clz);
      if( argClass.subtypeOf(_pool.get("water.Freezable")) ) return 9;
      if( argClass.subtypeOf(_pool.get("java.lang.Enum")) ) return 10;
      break;
    case '[':                   // Arrays
      return ftype(ct, sig.substring(1))+20; // Same as prims, plus 20
    }
    throw barf(ct, sig);
  }

  // Replace 2-byte strings like "%s" with s2.
  static private void subsub( StringBuilder sb, String s1, String s2 ) {
    int idx;
    while( (idx=sb.indexOf(s1)) != -1 ) sb.replace(idx,idx+2,s2);
  }


  private static Error barf( CtClass ct, String sig ) {
    return new Error(ct.getSimpleName()+"."+sig+": Serialization not implemented");
  }
}
