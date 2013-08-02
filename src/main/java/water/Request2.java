package water;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

import water.api.Request;

public abstract class Request2 extends Request {
  @Override protected void registered() {
    try {
      ArrayList<Class> classes = new ArrayList<Class>();
      {
        Class c = getClass();
        while( c != null ) {
          classes.add(c);
          c = c.getSuperclass();
        }
      }
      // Fields from parent classes first
      Collections.reverse(classes);
      ArrayList<Field> fields = new ArrayList<Field>();
      for( Class c : classes )
        for( Field field : c.getDeclaredFields() )
          if( !Modifier.isStatic(field.getModifiers()) )
            fields.add(field);

      for( Field f : fields ) {
        Annotation[] as = f.getAnnotations();
        API api = find(as, API.class);

        if( api != null ) {
          f.setAccessible(true);
          Object defaultValue = f.get(this);

          // Create an Argument instance to reuse existing Web framework for now
          Argument arg = null;

          if( Argument.class.isAssignableFrom(api.filter()) )
            arg = (Argument) getInnerClassConstructor(api.filter()).newInstance(this);
          else {
            // Real
            if( f.getType() == float.class || f.getType() == double.class ) {
              double val = ((Number) defaultValue).doubleValue();
              arg = new Real(f.getName(), api.required(), val, null, null, api.help());
            }

            // LongInt
            else if( f.getType() == int.class || f.getType() == long.class ) {
              long val = ((Number) defaultValue).longValue();
              arg = new LongInt(f.getName(), api.required(), val, null, null, api.help());
            }

            // Bool
            else if( f.getType() == boolean.class ) {
              boolean val = (Boolean) defaultValue;
              arg = new Bool(f.getName(), val, api.help());
            }
          }

          if( ColumnSelect.class.isAssignableFrom(api.filter()) ) {
            ColumnSelect name = (ColumnSelect) api.filter().newInstance();
            H2OHexKey key = null;
            for( Argument a : _arguments )
              if( a instanceof H2OHexKey && name._key.equals(((H2OHexKey) a)._name) )
                key = (H2OHexKey) a;
            arg = new HexAllColumnSelect(f.getName(), key);
          }

          if( arg != null ) {
            arg._name = f.getName();
            arg._required = api.required();
            arg._field = f;
          }
        }
      }
    } catch( Exception e ) {
      throw new RuntimeException(e);
    }
  }

  private static <T> T find(Annotation[] as, Class<T> c) {
    for( Annotation a : as )
      if( a.annotationType() == c )
        return (T) a;
    return null;
  }

  private static Constructor getInnerClassConstructor(Class cl) throws Exception {
    for( Constructor c : cl.getConstructors() ) {
      Class[] ps = c.getParameterTypes();
      if( ps.length == 1 && Request2.class.isAssignableFrom(ps[0]) )
        return c;
    }
    throw new Exception("Class " + cl.getName() + " must have an empty constructor");
  }

  // Create an instance per call instead of ThreadLocals
  @Override protected Request create(Properties parms) {
    Request2 request;
    try {
      request = getClass().newInstance();
      request._arguments = _arguments;
    } catch( Exception e ) {
      throw new RuntimeException(e);
    }
    return request;
  }

  public void set(Argument arg, Object value) {
    try {
      if( arg._field.getType() == Key.class && value instanceof ValueArray )
        value = ((ValueArray) value)._key;
      if( arg._field.getType() == int.class && value instanceof Long )
        value = ((Long) value).intValue();
      if( arg._field.getType() == float.class && value instanceof Double )
        value = ((Double) value).floatValue();
      if( value instanceof NumberSequence ) {
        double[] ds = ((NumberSequence) value)._arr;
        if( arg._field.getType() == int[].class ) {
          int[] is = new int[ds.length];
          for( int i = 0; i < is.length; i++ )
            is[i] = (int) ds[i];
          value = is;
        } else
          value = ds;
      }
      arg._field.set(this, value);
    } catch( Exception e ) {
      throw new RuntimeException(e);
    }
  }
}
