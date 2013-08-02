package water;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

import water.api.Request;

public abstract class Request2 extends Request {
  @Override protected void registered() {
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
      Input input = find(as, Input.class);

      if( input != null ) {
        f.setAccessible(true);
        Object defaultValue;
        try {
          defaultValue = f.get(this);
        } catch( Exception e ) {
          throw new RuntimeException(e);
        }

        API api = find(as, API.class);
        ExistingHexKey hexKey = find(as, ExistingHexKey.class);
        Bounds bounds = find(as, Bounds.class);
        ColumnSelect cols = find(as, ColumnSelect.class);
        Sequence seq = find(as, Sequence.class);
        Argument arg = null;

        // H2OHexKey
        if( f.getType() == Key.class ) {
          if( hexKey != null )
            arg = new H2OHexKey(f.getName());
          else
            arg = new H2OKey(f.getName(), input.required());
        }

        // Real
        else if( f.getType() == float.class || f.getType() == double.class ) {
          double val = ((Number) defaultValue).doubleValue();
          Double min = bounds != null ? bounds.min() : null;
          Double max = bounds != null ? bounds.max() : null;
          arg = new Real(f.getName(), input.required(), val, min, max, api.help());
        }

        // LongInt
        else if( f.getType() == int.class || f.getType() == long.class ) {
          long val = ((Number) defaultValue).longValue();
          Long min = bounds != null ? (long) bounds.min() : null;
          Long max = bounds != null ? (long) bounds.max() : null;
          arg = new LongInt(f.getName(), input.required(), val, min, max, api.help());
        }

        // Bool
        else if( f.getType() == boolean.class ) {
          boolean val = (Boolean) defaultValue;
          arg = new Bool(f.getName(), val, api.help());
        }

        // NumberSequence & HexAllColumnSelect
        else if( f.getType() == int[].class ) {
          if( seq != null ) {
            NumberSequence val = new NumberSequence(seq.pattern(), seq.mult(), 0);
            arg = new RSeq(f.getName(), input.required(), val, seq.mult());
          } else if( cols != null ) {
            H2OHexKey key = null;
            for( Argument t : _arguments )
              if( t instanceof H2OHexKey && cols.key().equals(((H2OHexKey) t)._name) )
                key = (H2OHexKey) t;
            arg = new HexAllColumnSelect(f.getName(), key);
          }
        }

        arg._field = f;
      }
    }
  }

  private static <T> T find(Annotation[] as, Class<T> c) {
    for( Annotation a : as )
      if( a.annotationType() == c )
        return (T) a;
    return null;
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
