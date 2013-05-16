package water;

import java.io.File;
import java.util.*;

import water.util.Log;
import water.util.Utils;

public class TypeMap {
  static public final short NULL = (short) -1;
  static public final short PRIM_B = 1;
  static public final short VALUE_ARRAY;

  static private final HashMap<String, Integer> MAP = new HashMap();
  static {
    int va_id = -1;
    for( int i = 0; i < TypeMapGen.CLAZZES.length; i++ ) {
      MAP.put(TypeMapGen.CLAZZES[i], i);
      if( TypeMapGen.CLAZZES[i].equals("water.ValueArray") ) va_id = i;
    }
    VALUE_ARRAY = (short) va_id; // Pre-cached the type id for ValueArray
  }

  static public int onLoad(String className) {
    Integer I = MAP.get(className);
    if( I == null ) throw Log.err(new RuntimeException("TypeMap missing " + className));
    return I;
  }

  static private final Freezable[] GOLD = new Freezable[TypeMapGen.CLAZZES.length];

  static public Iced newInstance(int id) {
    Iced f = (Iced) GOLD[id];
    if( f == null ) {
      try {
        GOLD[id] = f = (Iced) Class.forName(TypeMapGen.CLAZZES[id]).newInstance();
      } catch( Exception e ) {
        throw Log.errRTExcept(e);
      }
    }
    return f.newInstance();
  }

  static public Freezable newFreezable(int id) {
    Freezable f = GOLD[id];
    if( f == null ) {
      try {
        GOLD[id] = f = (Freezable) Class.forName(TypeMapGen.CLAZZES[id]).newInstance();
      } catch( Exception e ) {
        throw Log.errRTExcept(e);
      }
    }
    return f.newInstance();
  }

  static public String className(int id) {
    return TypeMapGen.CLAZZES[id];
  }

  static public Class clazz(int id) {
    if( GOLD[id] == null ) newInstance(id);
    return GOLD[id].getClass();
  }

  //

  public static void main(String[] args) throws Exception {
    Log._dontDie = true; // Ignore fatal class load error
    ArrayList<String> list = new ArrayList<String>();
    for( String name : Boot.getClasses() ) {
      if( !name.equals("water.api.RequestServer") && !name.equals("water.External") && !name.startsWith("water.r.") ) {
        Class c = Class.forName(name);
        if( Freezable.class.isAssignableFrom(c) ) list.add(c.getName());
      }
    }
    Collections.sort(list);
    String s = "" + //
        "package water;\n" + //
        "\n" + //
        "// Do not edit - generated\n" + //
        "public class TypeMapGen {\n" + //
        "  static final String[] CLAZZES = {\n" + //
        "    \" BAD\",                     // 0: BAD\n" +        //
        "    \"[B\",                       // 1: Array of Bytes\n";
    for( String c : list )
      s += "    \"" + c + "\",\n";
    s += "  };\n";
    s += "}";
    Utils.writeFile(new File("src/main/java/water/TypeMapGen.java"), s);
    Log.info("Generated TypeMap");
  }
}
