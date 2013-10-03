package water;

import hex.GridSearch;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

import water.api.*;
import water.fvec.Frame;
import water.util.Utils;

public abstract class Request2 extends Request {
  transient Properties _parms;

  public String input(String fieldName) {
    return _parms.getProperty(fieldName);
  }

  public class TypeaheadKey extends TypeaheadInputText<Key> {
    transient Key _defaultValue;
    transient Class _type;

    public TypeaheadKey() {
      this(null, true);
    }

    public TypeaheadKey(Class type, boolean required) {
      super(TypeaheadKeysRequest.class, "", required);
      _type = type;
      setRefreshOnChange();
    }

    public void setValue(Key key) {
      record()._value = key;
      record()._originalValue = key.toString();
    }

    @Override protected Key parse(String input) {
      Key k = Key.make(input);
      if( _type != null ) {
        Value v = DKV.get(k);
        if( v != null && !compatible(_type, v.get()) )
          throw new IllegalArgumentException(input + ":" + errors()[0]);
        if( v == null && _required )
          throw new IllegalArgumentException("Key '" + input + "' does not exist!");
      }
      return k;
    }

    @Override protected Key defaultValue() {
      return _defaultValue;
    }

    @Override protected String queryDescription() {
      return "A key" + (_type != null ? " of type " + _type.getSimpleName() : "");
    }

    @Override protected String[] errors() {
      return new String[] { "Key is not a " + _type.getSimpleName() };
    }
  }

  /**
   * Fields that depends on another, e.g. select Vec from a Frame.
   */
  public class Dependent implements Filter {
    public final String _ref;

    protected Dependent(String name) {
      _ref = name;
    }

    @Override public boolean run(Object value) {
      return true;
    }
  }

  public class ColumnSelect extends Dependent {
    protected ColumnSelect(String key) {
      super(key);
    }
  }

  public class VecSelect extends Dependent {
    protected VecSelect(String key) {
      super(key);
    }
  }

  public class VecClassSelect extends Dependent {
    protected VecClassSelect(String key) {
      super(key);
    }
  }

  public class MultiVecSelect extends Dependent {
    protected MultiVecSelect(String key) {
      super(key);
    }
  }

  /**
   * Iterates over fields and their annotations, and creates argument handlers.
   */
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

      HashMap<String, FrameClassVec> classVecs = new HashMap<String, FrameClassVec>();
      for( Field f : fields ) {
        Annotation[] as = f.getAnnotations();
        API api = find(as, API.class);

        if( api != null && Helper.isInput(api) ) {
          f.setAccessible(true);
          Object defaultValue = f.get(this);

          // Create an Argument instance to reuse existing Web framework for now
          Argument arg = null;

          // simplest case, filter is an Argument
          if( Argument.class.isAssignableFrom(api.filter()) )
            arg = (Argument) newInstance(api);

          // String
          else if( f.getType() == String.class )
            arg = new Str(f.getName(), (String) defaultValue);

          // Real
          else if( f.getType() == float.class || f.getType() == double.class ) {
            double val = ((Number) defaultValue).doubleValue();
            arg = new Real(f.getName(), api.required(), val, api.dmin(), api.dmax(), api.help());
          }

          // LongInt
          else if( f.getType() == int.class || f.getType() == long.class ) {
            long val = ((Number) defaultValue).longValue();
            arg = new LongInt(f.getName(), api.required(), val, api.lmin(), api.lmax(), api.help());
          }

          // Bool
          else if( f.getType() == boolean.class ) {
            boolean val = (Boolean) defaultValue;
            arg = new Bool(f.getName(), val, api.help());
          }

          // Enum
          else if( Enum.class.isAssignableFrom(f.getType()) ) {
            Enum val = (Enum) defaultValue;
            arg = new EnumArgument(f.getName(), val);
          }

          // Key
          else if( f.getType() == Key.class ) {
            TypeaheadKey t = new TypeaheadKey();
            t._defaultValue = (Key) defaultValue;
            arg = t;
          }

          // Auto-cast from key to Iced field
          else if( Freezable.class.isAssignableFrom(f.getType()) && api.filter() == Default.class )
            arg = new TypeaheadKey(f.getType(), api.required());

          //
          else if( ColumnSelect.class.isAssignableFrom(api.filter()) ) {
            ColumnSelect name = (ColumnSelect) newInstance(api);
            H2OHexKey key = null;
            for( Argument a : _arguments )
              if( a instanceof H2OHexKey && name._ref.equals(((H2OHexKey) a)._name) )
                key = (H2OHexKey) a;
            arg = new HexAllColumnSelect(f.getName(), key);
          }

          //
          else if( Dependent.class.isAssignableFrom(api.filter()) ) {
            Dependent d = (Dependent) newInstance(api);
            Argument ref = find(d._ref);
            if( d instanceof VecSelect )
              arg = new FrameKeyVec(f.getName(), (TypeaheadKey) ref);
            else if( d instanceof VecClassSelect ) {
              arg = new FrameClassVec(f.getName(), (TypeaheadKey) ref);
              classVecs.put(d._ref, (FrameClassVec) arg);
            } else if( d instanceof MultiVecSelect ) {
              FrameClassVec response = classVecs.get(d._ref);
              arg = new FrameKeyMultiVec(f.getName(), (TypeaheadKey) ref, response, api.help());
            }
          }

          if( arg != null ) {
            arg._name = f.getName();
            arg._required = api.required();
            arg._field = f;
            arg._hideInQuery = api.hide();
          }
        }
      }
    } catch( Exception e ) {
      throw new RuntimeException(e);
    }
  }

  final Argument find(String name) {
    for( Argument a : _arguments )
      if( name.equals(a._name) )
        return a;
    return null;
  }

  // Extracted in separate class as Weaver cannot load REquest during boot
  static final class Helper {
    static boolean isInput(API api) {
      return api.filter() != Filter.class || api.filters().length != 0;
    }
  }

  private static <T> T find(Annotation[] as, Class<T> c) {
    for( Annotation a : as )
      if( a.annotationType() == c )
        return (T) a;
    return null;
  }

  private Filter newInstance(API api) throws Exception {
    for( Constructor c : api.filter().getDeclaredConstructors() ) {
      c.setAccessible(true);
      Class[] ps = c.getParameterTypes();
      if( ps.length == 1 && RequestArguments.class.isAssignableFrom(ps[0]) )
        return (Filter) c.newInstance(this);
    }
    for( Constructor c : api.filter().getDeclaredConstructors() ) {
      Class[] ps = c.getParameterTypes();
      if( ps.length == 0 )
        return (Filter) c.newInstance();
    }
    throw new Exception("Class " + api.filter().getName() + " must have an empty constructor");
  }

  // Create an instance per call instead of ThreadLocals
  @Override protected Request create(Properties parms) {
    Request2 request;
    try {
      request = getClass().newInstance();
      request._arguments = _arguments;
      request._parms = parms;
    } catch( Exception e ) {
      throw new RuntimeException(e);
    }
    return request;
  }

  // Expand grid search related argument sets
  @Override protected NanoHTTPD.Response serveGrid(NanoHTTPD server, Properties parms, RequestType type) {
    // TODO: real parser for unified imbricated argument sets, expressions etc
    String[][] values = new String[_arguments.size()][];
    boolean gridSearch = false;
    for( int i = 0; i < _arguments.size(); i++ ) {
      String value = _parms.getProperty(_arguments.get(i)._name);
      if( value != null ) {
        int off = 0;
        int next = 0;
        while( (next = value.indexOf('|', off)) >= 0 ) {
          if( next != off )
            values[i] = Utils.add(values[i], value.substring(off, next));
          off = next + 1;
          gridSearch = true;
        }
        if( off < value.length() )
          values[i] = Utils.add(values[i], value.substring(off));
      }
    }
    if( !gridSearch )
      return superServeGrid(server, parms, type);

    // Ignore destination key so that each job gets its own
    _parms.remove("destination_key");
    for( int i = 0; i < _arguments.size(); i++ )
      if( _arguments.get(i)._name.equals("destination_key") )
        values[i] = null;

    // Iterate over all argument combinations
    int[] counters = new int[values.length];
    ArrayList<Job> jobs = new ArrayList<Job>();
    for( ;; ) {
      Job job = (Job) create(_parms);
      Properties combination = new Properties();
      for( int i = 0; i < values.length; i++ ) {
        if( values[i] != null ) {
          String value = values[i][counters[i]];
          value = value.trim();
          combination.setProperty(_arguments.get(i)._name, value);
          _arguments.get(i).reset();
          _arguments.get(i).check(job, value);
        }
      }
      job._parms = combination;
      jobs.add(job);
      if( !increment(counters, values) )
        break;
    }
    GridSearch grid = new GridSearch();
    grid.jobs = jobs.toArray(new Job[jobs.size()]);
    return grid.superServeGrid(server, parms, type);
  }

  public final NanoHTTPD.Response superServeGrid(NanoHTTPD server, Properties parms, RequestType type) {
    return super.serveGrid(server, parms, type);
  }

  private static boolean increment(int[] counters, String[][] values) {
    for( int i = 0; i < counters.length; i++ ) {
      if( values[i] != null && counters[i] < values[i].length - 1 ) {
        counters[i]++;
        return true;
      } else
        counters[i] = 0;
    }
    return false;
  }

  /*
   * Arguments to fields casts.
   */

  private static boolean compatible(Class type, Object o) {
    if( type == Frame.class && o instanceof ValueArray )
      return true;
    return type.isInstance(o);
  }

  public void set(Argument arg, String input, Object value) {
    if( arg._field.getType() != Key.class && value instanceof Key )
      value = UKV.get((Key) value);

    try {
      if( arg._field.getType() == Key.class && value instanceof ValueArray )
        value = ((ValueArray) value)._key;
      //
      else if( arg._field.getType() == int.class && value instanceof Long )
        value = ((Long) value).intValue();
      //
      else if( arg._field.getType() == float.class && value instanceof Double )
        value = ((Double) value).floatValue();
      //
      else if( arg._field.getType() == Frame.class && value instanceof ValueArray )
        value = ((ValueArray) value).asFrame(input);
      //
      else if( value instanceof NumberSequence ) {
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
