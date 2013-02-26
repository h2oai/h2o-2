package water;

import javassist.*;

public class Weaver {
  private final ClassPool _pool;
  private final CtClass _enum;
  private final CtClass[] _serBases;
  Weaver() {
    try {
      _pool = ClassPool.getDefault();
      _enum = _pool.get("java.lang.Enum");
      _serBases = new CtClass[] {
          _pool.get("water.Iced"),
          _pool.get("water.DTask"),
          _enum,
      };

      for( CtClass c : _serBases ) c.freeze();
    } catch( Exception e ) {
      throw new RuntimeException(e);
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
  public synchronized CtClass javassistLoadClass(String name) {
    try {
      if( name.equals("water.Boot")) return null;
      CtClass cc = _pool.get(name); // Full Name Lookup
      if( cc == null ) return null; // Oops?  Try the system loader, but expected to work
      String pack = cc.getPackageName();
      if( !pack.startsWith("water") &&
          !pack.startsWith("hex") &&
          !pack.startsWith("org.junit") &&
          true ) return null; // Not in my package

      for( CtClass base : _serBases )
        if( cc.subclassOf(base) )
          return javassistLoadClass(cc);
      return cc;
    } catch( NotFoundException nfe ) {
      return null;              // Not found?  Use the normal loader then
    } catch( CannotCompileException cce ) { // Expected to compile
      throw new RuntimeException(cce);
    }
  }

  public synchronized CtClass javassistLoadClass( CtClass cc ) throws NotFoundException, CannotCompileException {
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
  CtClass addSerializationMethods( CtClass cc ) throws CannotCompileException, NotFoundException {
    if( cc.subclassOf(_enum) ) {
      exposeRawEnumArray(cc);
    } else {
      cc.setModifiers(javassist.Modifier.setPublic(cc.getModifiers()));
      ensureSerMethods(cc);
      ensureNullaryCtor(cc);
      ensureNewInstance(cc);
      ensureType(cc);
    }
    cc.freeze();
    return cc;
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

  private void ensureType(CtClass cc) throws NotFoundException, CannotCompileException {
    CtMethod ccms[] = cc.getDeclaredMethods();
    if( !hasExisting("frozenType", "()I", ccms) ) {
      String field = "__h2o_type";
      cc.addField(CtField.make("static int " + field + " = -1;", cc));
      cc.addMethod(CtNewMethod.make(
          "public int frozenType() {" +
          "  if(" + field + "<0)" + field + "=water.Types.id(" + cc.getName() + ".class);" +
          "  return " + field + ";" +
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
    // RuntimeException (to make sure we noisily fail instead of silently
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
