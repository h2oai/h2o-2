package water;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import water.util.Log;
import water.util.Utils;

public class TypeMap {
  static public final short NULL = (short) -1;
  static public final short PRIM_B = 1;
  static public final short VALUE_ARRAY;
  static public final short C1VECTOR;

  static private final HashMap<String, Integer> MAP = new HashMap();
  static private final String[] CLAZZES;
  static private final Freezable[] GOLD;
  static {
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    InputStream in = cl.getResourceAsStream("typemap");
    int va_id = -1, c0_id = -1, i = 0;
    if( in != null ) {
      BufferedReader r = new BufferedReader(new InputStreamReader(in));
      try {
        for( ;; ) {
          String line = r.readLine();
          if( line == null || line.length() == 0 ) break;
          if( line.equals("water.ValueArray") ) va_id = i;
          if( line.equals("water.fvec.C1Vector") ) c0_id = i;
          MAP.put(line, i++);
        }
      } catch( IOException e ) {
        throw new RuntimeException(e);
      }
    }
    VALUE_ARRAY = (short) va_id; // Pre-cached the type id for ValueArray
    C1VECTOR    = (short) c0_id; // Pre-cached the type id for C1Vector
    GOLD = new Freezable[i];
    CLAZZES = new String[i];
    for( Entry<String, Integer> entry : MAP.entrySet() )
      CLAZZES[entry.getValue()] = entry.getKey();
  }

  static public int onLoad(String className) {
    Integer I = MAP.get(className);
    if( I == null ) throw Log.err(new RuntimeException("TypeMap missing " + className));
    return I;
  }

  static public Iced newInstance(int id) {
    Iced f = (Iced) GOLD[id];
    if( f == null ) {
      try {
        GOLD[id] = f = (Iced) Class.forName(CLAZZES[id]).newInstance();
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
        GOLD[id] = f = (Freezable) Class.forName(CLAZZES[id]).newInstance();
      } catch( Exception e ) {
        throw Log.errRTExcept(e);
      }
    }
    return f.newInstance();
  }

  static public String className(int id) {
    return CLAZZES[id];
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
        " BAD\n" +        //
        "[B\n";
    for( String c : list )
      s += c + "\n";
    Utils.writeFile(new File("src/main/resources/typemap"), s);
    Log.info("Generated TypeMap");
  }
}
