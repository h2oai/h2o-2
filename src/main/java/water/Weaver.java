package water;

import java.util.*;

import javassist.*;
import javassist.bytecode.*;
import javassist.bytecode.SignatureAttribute.ClassSignature;
import javassist.bytecode.SignatureAttribute.TypeArgument;
import water.api.Request.API;
import water.util.Log;
import water.util.Log.Tag.Sys;

public class Weaver {
  private final ClassPool _pool;
  private final CtClass _dtask, _iced, _enum, _freezable;
  private final CtClass[] _serBases;
  private final CtClass _fielddoc;
  private final CtClass _arg;
  // Versioning
//  private final CtClass _apiSchema;
//  private final CtClass _apiAdaptor;
//  private final CtClass _apiHandler;
  // ---
  public static Class _typeMap;
  public static volatile String[] _packages = new String[] { "water", "hex", "org.junit", "com.oxdata", "ai.h2o" };

  Weaver() {
    try {
      _pool = ClassPool.getDefault();
      _pool.insertClassPath(new ClassClassPath(Weaver.class));
      _iced = _pool.get("water.Iced"); // Needs serialization
      _dtask= _pool.get("water.DTask");// Needs serialization and remote execution
      _enum = _pool.get("java.lang.Enum"); // Needs serialization
      _freezable = _pool.get("water.Freezable"); // Needs serialization
//      _apiSchema = _pool.get("water.api.rest.schemas.ApiSchema");
//      _apiAdaptor = _pool.get("water.api.rest.ApiAdaptor");
//      _apiHandler = _pool.get("water.api.rest.handlers.AbstractHandler");
      //_versioned = _pool.get("water.api.rest.REST$Versioned");
      _serBases = new CtClass[] { _iced, _dtask, _enum, _freezable };
      for( CtClass c : _serBases ) c.freeze();
      _fielddoc = _pool.get("water.api.DocGen$FieldDoc");// Is auto-documentation result
      _arg  = _pool.get("water.api.RequestArguments$Argument"); // Needs auto-documentation
    } catch( NotFoundException e ) {
      throw new RuntimeException(e);
    }
  }

  public static void registerPackage(String name) {
    synchronized( Weaver.class ) {
      String[] a = _packages;
      if(Arrays.asList(a).indexOf(name) < 0) {
        String[] t = Arrays.copyOf(a, a.length + 1);
        t[t.length-1] = name;
        _packages = t;
      }
    }
  }

  public Class weaveAndLoad(String name, ClassLoader cl) {
    try {
      CtClass w = javassistLoadClass(name);
      if( w == null ) return null;
      return w.toClass(cl, null);
    } catch( CannotCompileException e ) {
      throw new RuntimeException(e);
    }
  }

  // See if javaassist can find this class; if so then check to see if it is a
  // subclass of water.DTask, and if so - alter the class before returning it.
  private synchronized CtClass javassistLoadClass(String name) {
    // Always use this weaver's classloader to preserve correct top-level classloader
    // for loading H2O's classes.
    // The point is to load all the time weaved classes by the same classloader
    // and do not let JavaAssist to use thread context classloader.
    // For normal H2O execution it will be always the same classloader
    // but for running from 3rd party code, we preserve Boot's parent loader
    // for all H2O internal classes.
    final ClassLoader ccl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
    try {
      if( name.equals("water.Boot") ) return null;
      CtClass cc = _pool.get(name); // Full Name Lookup
      if( cc == null ) return null; // Oops?  Try the system loader, but expected to work
      if( !inPackages(cc.getPackageName()) ) return null;

      for( CtClass base : _serBases )
        if( cc.subclassOf(base) )
          return javassistLoadClass(cc);

      // Subtype of an alternative freezable?
      if( cc.subtypeOf( _freezable ) ) {
        // Find the alternative freezable base
        CtClass xcc = cc;
        CtClass ycc = null;
        while( xcc.subtypeOf(_freezable) ) { ycc = xcc; xcc = xcc.getSuperclass(); }
        if( !ycc.isFrozen() ) ycc.freeze(); // Freeze the alternative base
        return cc == ycc ? cc : javassistLoadClass(cc); // And weave the subclass
      }

      return cc;
    } catch( NotFoundException nfe ) {
      return null;              // Not found?  Use the normal loader then
    } catch( CannotCompileException e ) { // Expected to compile
      throw new RuntimeException(e);
    } catch (BadBytecode e) {
      throw new RuntimeException(e);
    } finally {
      // Do not forget to configure classloader back to original value
      Thread.currentThread().setContextClassLoader(ccl);
    }
  }

  private static boolean inPackages(String pack) {
    if( pack==null ) return false;
    String[] p = _packages;
    for( int i = 0; i < p.length; i++ )
      if( pack.startsWith(p[i]) )
        return true;
    return false;
  }

  private synchronized CtClass javassistLoadClass( CtClass cc ) throws NotFoundException, CannotCompileException, BadBytecode {
    if( cc.isFrozen() ) return cc;
    // serialize parent
    javassistLoadClass(cc.getSuperclass());

    // Serialize enums first, since we need the raw_enum function for this class
    for( CtField ctf : cc.getDeclaredFields() ) {
      CtClass base = ctf.getType();
      while( base.isArray() ) base = base.getComponentType();
      if( base.subclassOf(_enum) && base != cc )
        javassistLoadClass(base);
    }
    CtClass ccr = addSerializationMethods(cc);
    ccr.freeze();
    return ccr;
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
  CtClass addSerializationMethods( CtClass cc ) throws CannotCompileException, NotFoundException {
    if( cc.subclassOf(_enum) ) exposeRawEnumArray(cc);
    if( cc.subclassOf(_iced) ) ensureAPImethods(cc);
    if( cc.subclassOf(_iced) ||
        cc.subclassOf(_dtask)||
        cc.subtypeOf(_freezable)) {
      cc.setModifiers(javassist.Modifier.setPublic(cc.getModifiers()));
      ensureSerMethods(cc);
      ensureNullaryCtor(cc);
      ensureNewInstance(cc);
      ensureType(cc);
    }
    return cc;
  }


  // Expose the raw enum array that all Enums have, so we can directly convert
  // ordinal values to enum instances.
  private void exposeRawEnumArray(CtClass cc) throws NotFoundException, CannotCompileException {
      CtField field;
      try {
        field = cc.getField("$VALUES");
      } catch( NotFoundException nfe ) {
        // Eclipse apparently stores this in a different place.
        field = cc.getField("ENUM$VALUES");
      }
      String body = "public static "+cc.getName()+" raw_enum(int i) { return i==255?null:"+field.getName()+"[i]; } ";
      try {
        cc.addMethod(CtNewMethod.make(body,cc));
      } catch( CannotCompileException ce ) {
        Log.warn(Sys.WATER,"--- Compilation failure while compiler raw_enum for "+cc.getName()+"\n"+body+"\n------",ce);
        throw ce;
      }
  }

  // Create a newInstance call which will rapidly make a new object of a
  // particular type *without* Reflection's overheads.
  private void ensureNewInstance(CtClass cc) throws NotFoundException, CannotCompileException {
    CtMethod ccms[] = cc.getDeclaredMethods();
    if( !javassist.Modifier.isAbstract(cc.getModifiers()) &&
        !hasExisting("newInstance", "()Lwater/Freezable;", ccms) ) {
      cc.addMethod(CtNewMethod.make(
          "public water.Freezable newInstance() {\n" +
          "    return new " +cc.getName()+"();\n" +
          "}", cc));
    }
  }

  // Serialized types support a unique dense integer per-class, so we can do
  // simple array lookups to get class info.  The integer is cluster-wide
  // unique and determined lazily.
  private void ensureType(CtClass cc) throws NotFoundException, CannotCompileException {
    CtMethod ccms[] = cc.getDeclaredMethods();
    if( !javassist.Modifier.isAbstract(cc.getModifiers()) &&
        !hasExisting("frozenType", "()I", ccms) ) {
      // Build a simple field & method returning the type token
      cc.addField(new CtField(CtClass.intType, "_frozen$type", cc));
      cc.addMethod(CtNewMethod.make("public int frozenType() {" +
                                    "  return _frozen$type == 0 ? (_frozen$type=water.TypeMap.onIce(\""+cc.getName()+"\")) : _frozen$type;" +
                                    "}",cc));
    }
  }

  private void ensureVersion(CtClass cc) throws NotFoundException, CannotCompileException, BadBytecode {
    CtMethod ccms[] = cc.getDeclaredMethods();
    if (!javassist.Modifier.isAbstract(cc.getModifiers())) {
      String gsig = cc.getGenericSignature();
      ClassSignature csig = SignatureAttribute.toClassSignature(gsig);
      // Warning: this is not doing proper parent (superclass/interfaces) traversal
      TypeArgument ta = getTypeArg(csig.getSuperClass().getTypeArguments(), "Lwater/api/rest/Version");
      if (ta!=null && !hasExisting("getVersion", "()"+ta.getType().encode(), ccms) ) {
        String typeName = ta.toString();
        String valueName = getValueFromType(typeName);
        //cc.addMethod(CtNewMethod.make("public "+typeName+" getVersion() {" +
        cc.addMethod(CtNewMethod.make("public water.api.rest.Version getVersion() {" +
            "  return "+valueName+";" +
            "}",cc));
      }
    }
  }
  private String getValueFromType(String typeName) {
    int idx = typeName.indexOf('$');
    String t = typeName.substring(0, idx);
    String v = typeName.substring(idx+1).toLowerCase();
    return t+"."+v;
  }
  private TypeArgument getTypeArg(TypeArgument[] args, String prefix) {
    for (TypeArgument ta : args)
      if (ta.getType().encode().startsWith(prefix)) return ta;
    return null;
  }


  // --------------------------------------------------------------------------
  private static abstract class FieldFilter {
    abstract boolean filter( CtField ctf ) throws NotFoundException;
  }
  private void ensureAPImethods(CtClass cc) throws NotFoundException, CannotCompileException {
    CtField ctfs[] = cc.getDeclaredFields();
    boolean api = false;
    for( CtField ctf : ctfs )
      if( ctf.getName().equals("API_WEAVER") ) {
        api = true; break;
      }
    if( api == false ) return;

    CtField fielddoc=null;
    CtField getdoc=null;
    boolean callsuper = true;
    for( CtClass base : _serBases )
      if( cc.getSuperclass() == base ) callsuper = false;

    // ---
    // Auto-gen JSON output to AutoBuffers
    make_body(cc,ctfs,callsuper,
              "public water.AutoBuffer writeJSONFields(water.AutoBuffer ab) {\n",
              "  super.writeJSONFields(ab)",
              "  ab.putJSON%z(\"%s\",%s)",
              "  ab.putEnumJSON(\"%s\",%s)",
              "  ab.putJSON%z(\"%s\",%s)",
              ".put1(',');\n",
              ";\n  return ab;\n}",
              new FieldFilter() {
                @Override boolean filter(CtField ctf) throws NotFoundException {
                  API api = null;
                  try {
                    api = (API) ctf.getAnnotation(API.class);
                  } catch( ClassNotFoundException ex) { throw new NotFoundException("getAnnotations throws ", ex); }
                  return api != null && (api.json() || !isInput(ctf.getType(), api));
                }
              });

    // ---

    // Auto-gen JSON & Args doc method.  Requires a structured java object.
    // Every @API annotated field is either a JSON field, an Argument, or both.
    // field, and has some associated fields.
    //
    //     H2OHexKey someField2; // Anything derived from RequestArguments$Argument
    //     static final String someField2Help = "some help text";
    //     static final int someField2MinVar = 1, someField2MaxVar = 1;
    //
    //     String[] someField; // Anything NOT derived from Argument is a JSON field
    //     static final String someFieldHelp = "some help text";
    //     static final int someFieldMinVar = 1, someFieldMaxVar = 1;
    // xxxMinVar and xxxMaxVar are optional; if xxxMinVar is missing it
    // defaults to 1, and if xxxMaxVar is missing it defaults "till now".
    StringBuilder sb = new StringBuilder();
    sb.append("new water.api.DocGen$FieldDoc[] {");
    // Get classes in the hierarchy with marker field
    ArrayList<CtClass> classes = new ArrayList<CtClass>();
    CtClass current = cc;
    while( true ) {             // For all self & superclasses
      classes.add(current);
      current = current.getSuperclass();
      api = false;
      for( CtField ctf : current.getDeclaredFields() )
        if( ctf.getName().equals("API_WEAVER") )
          api = true;
      if( api == false ) break;
    }
    // Start with parent classes to get fields in order
    Collections.reverse(classes);
    boolean first = true;
    for(CtClass c : classes) {
      for( CtField ctf : c.getDeclaredFields() ) {
        int mods = ctf.getModifiers();
        if( javassist.Modifier.isStatic(mods) ) {
          if( c == cc ) {     // Capture the DOC_* fields for self only
            if( ctf.getName().equals("DOC_FIELDS") ) fielddoc = ctf;
            if( ctf.getName().equals("DOC_GET") ) getdoc = ctf;
          }
          continue;  // Only auto-doc instance fields (not static)
        }
        first = addDocIfAPI(sb,ctf,cc,first);
      }
    }

    sb.append("}");
    if( fielddoc == null ) throw new CannotCompileException("Did not find static final DocGen.FieldDoc[] DOC_FIELDS field;");
    if( !fielddoc.getType().isArray() ||
        fielddoc.getType().getComponentType() != _fielddoc )
      throw new CannotCompileException("DOC_FIELDS not declared static final DocGen.FieldDoc[];");
    cc.removeField(fielddoc);    // Remove the old one
    cc.addField(fielddoc,CtField.Initializer.byExpr(sb.toString()));
    cc.addMethod(CtNewMethod.make("  public water.api.DocGen$FieldDoc[] toDocField() { return DOC_FIELDS; }",cc));
    if( getdoc != null )
      cc.addMethod(CtNewMethod.make("  public String toDocGET() { return DOC_GET; }",cc));
  }

  private boolean addDocIfAPI( StringBuilder sb, CtField ctf, CtClass cc, boolean first ) throws NotFoundException, CannotCompileException {
    String name = ctf.getName();
    Object[] as;
    try { as = ctf.getAnnotations(); }
    catch( ClassNotFoundException ex) { throw new NotFoundException("getAnnotations throws ", ex); }
    API api = null;
    for(Object o : as) if(o instanceof API)  api = (API) o;
    if( api != null ) {
      String help = api.help();
      int min = api.since();
      int max = api.until();
      if( min < 1 || min > 1000000 ) throw new CannotCompileException("Found field '"+name+"' but 'since' < 1 or 'since' > 1000000");
      if( max < min || (max > 1000000 && max != Integer.MAX_VALUE) )
        throw new CannotCompileException("Found field '"+name+"' but 'until' < "+min+" or 'until' > 1000000");

      if( first ) first = false;
      else sb.append(",");
      boolean input = isInput(ctf.getType(), api);
      sb.append("new water.api.DocGen$FieldDoc(\""+name+"\",\""+help+"\","+min+","+max+","+ctf.getType().getName()+".class,"+input+","+api.required()+",water.api.ParamImportance."+api.importance()+",water.api.Direction."+api.direction()+",\""+api.path()+"\","+ api.type().getName()+".class,\""+api.valid()+"\", \""+api.enabled()+"\",\""+api.visible()+"\")");
    }
    return first;
  }

  private final boolean isInput(CtClass fieldType, API api) {
    return Request2.Helper.isInput(api) || //
      // Legacy
      fieldType.subclassOf(_arg);
  }

  // --------------------------------------------------------------------------
  // Support for a nullary constructor, for deserialization.
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

  // Serialization methods: read, write & copyOver.
  private void ensureSerMethods(CtClass cc) throws NotFoundException, CannotCompileException {
    // Check for having "read" and "write".  Either All or None of read & write
    // must be defined.  Note that I use getDeclaredMethods which returns only
    // the local methods.  The singular getDeclaredMethod searches for a
    // specific method *up into superclasses*, which will trigger premature
    // loading of those superclasses.
    CtMethod ccms[] = cc.getDeclaredMethods();
    boolean w = hasExisting("write", "(Lwater/AutoBuffer;)Lwater/AutoBuffer;", ccms);
    boolean r = hasExisting("read" , "(Lwater/AutoBuffer;)Lwater/Freezable;" , ccms);
    boolean d = cc.subclassOf(_dtask); // Subclass of DTask?
    boolean c = hasExisting("copyOver" , "(Lwater/Freezable;)V" , ccms);
    if( w && r && (!d || c) ) return;
    if( w || r || c )
      throw new RuntimeException(cc.getName() +" must implement all of " +
      "read(AutoBuffer) and write(AutoBuffer) and copyOver(Freezable) or none");

    // Add the serialization methods: read, write.
    CtField ctfs[] = cc.getDeclaredFields();

    // We cannot call Iced.xxx, as these methods always throw a
    // RuntimeException (to make sure we noisily fail instead of silently
    // fail).  But we DO need to call the super-chain of serialization methods
    // - stopping at DTask.
    boolean callsuper = true;
//    for( CtClass base : _serBases )
//      if( cc.getSuperclass() == base ) callsuper = false;
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
    // TODO use Freezable.write instead of AutoBuffer.put for final classes
    make_body(cc,ctfs,callsuper,
              "public water.AutoBuffer write(water.AutoBuffer ab) {\n",
              "  super.write(ab);\n",
              "  ab.put%z(%s);\n",
              "  ab.putEnum(%s);\n",
              "  ab.put%z(%s);\n",
              "",
              "  return ab;\n" +
              "}", null);

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
              "",
              "  return this;\n" +
              "}", null);

    // Build a copyOver method that looks something like this:
    //     public void copyOver( T s ) {
    //       _x = s._x;
    //       _xs = s._xs;
    //       _d = s._d;
    //     }
    if( d ) make_body(cc,ctfs,callsuper,
              "public void copyOver(water.Freezable i) {\n"+
              "  "+cc.getName()+" s = ("+cc.getName()+")i;\n",
              "  super.copyOver(s);\n",
              "  %s = s.%s;\n",
              "  %s = s.%s;\n",
              "  %s = s.%s;\n",
              "",
              "}", null);

  }

  // Produce a code body with all these fill-ins.
  private final void make_body(CtClass cc, CtField[] ctfs, boolean callsuper,
                               String header,
                               String supers,
                               String prims,
                               String enums,
                               String freezables,
                               String field_sep,
                               String trailer,
                               FieldFilter ff
                               ) throws CannotCompileException, NotFoundException {
    StringBuilder sb = new StringBuilder();
    sb.append(header);
    if( callsuper ) sb.append(supers);
    boolean debug_print = false;
    boolean first = !callsuper;
    for( CtField ctf : ctfs ) {
      int mods = ctf.getModifiers();
      if( javassist.Modifier.isTransient(mods) || javassist.Modifier.isStatic(mods) ) {
        debug_print |= ctf.getName().equals("DEBUG_WEAVER");
        continue;  // Only serialize not-transient instance fields (not static)
      }
      if( ff != null && !ff.filter(ctf) ) continue; // Fails the filter
      if( first ) first = false;
      else sb.append(field_sep);

      CtClass base = ctf.getType();
      while( base.isArray() ) base = base.getComponentType();

      int ftype = ftype(ctf.getSignature(), cc, ctf );   // Field type encoding
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
      System.err.println(cc.getName()+" "+body);
    }

    try {
      cc.addMethod(CtNewMethod.make(body,cc));
    } catch( CannotCompileException e ) {
      throw Log.err("--- Compilation failure while compiling serializers for "+cc.getName()+"\n"+body+"\n-----",e);
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
  private int ftype( String sig, CtClass ct, CtField fld ) throws NotFoundException {
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
      return ftype(sig.substring(1), ct, fld)+20; // Same as prims, plus 20
    }
    throw barf(ct, fld);
  }

  // Replace 2-byte strings like "%s" with s2.
  static private void subsub( StringBuilder sb, String s1, String s2 ) {
    int idx;
    while( (idx=sb.indexOf(s1)) != -1 ) sb.replace(idx,idx+2,s2);
  }


  private static RuntimeException barf( CtClass ct, CtField fld ) throws NotFoundException {
    return new RuntimeException(ct.getSimpleName()+"."+fld.getName()+" of type "+(fld.getType().getSimpleName())+": Serialization not implemented; does not extend Iced or DTask");
  }

}
