package water;

import dontweave.gson.JsonElement;
import dontweave.gson.JsonObject;
import dontweave.gson.JsonParser;
import hex.GridSearch;
import water.api.DocGen;
import water.api.Request;
import water.api.RequestArguments;
import water.api.RequestServer.API_VERSION;
import water.fvec.Vec;
import water.util.Log;
import water.util.Utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

public abstract class Request2 extends Request {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;

  protected transient Properties _parms;

  @API(help = "Response stats and info.")
  public ResponseInfo response_info;

  public String input(String fieldName) {
    return _parms == null ? null : _parms.getProperty(fieldName);
  }

  public class TypeaheadKey extends TypeaheadInputText<Key> {
    transient Key _defaultValue;
    transient Class _type;

    public TypeaheadKey() {
      this(null, true);
    }

    public TypeaheadKey(Class type, boolean required) {
      super(mapTypeahead(type), "", required);
      _type = type;
      setRefreshOnChange();
    }

    public void setValue(Key key) {
      record()._value = key;
      record()._originalValue = key.toString();
    }

    @Override protected Key parse(String input) {
      if (_validator!=null) _validator.validateRaw(input);
      Key k = Key.make(input);
      Value v = DKV.get(k);
      if( v == null && _mustExist )
        throw new H2OIllegalArgumentException(this, "Key '" + input + "' does not exist!");
      if( _type != null ) {
        if( v == null && _required )
          throw new H2OIllegalArgumentException(this, "Key '" + input + "' does not exist!");
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
      if( _type != null )
        return new String[] { "Key is not a " + _type.getSimpleName() };
      return super.errors();
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

  public class SpecialVecSelect extends VecSelect {
    public boolean optional = false;
    protected SpecialVecSelect(String key) { this(key,false);}
    protected SpecialVecSelect(String key, boolean optional) {
      super(key);
      this.optional = optional;
    }
  }

  public class VecClassSelect extends Dependent {
    protected VecClassSelect(String key) {
      super(key);
    }
  }

  /**
   * Specify how a column specifier field is parsed.
   */
  public enum MultiVecSelectType {
    /**
     * Treat a token as a column name. Otherwise, treat it as a 0-based index if it looks like a
     * positive integer.
     */
    NAMES_THEN_INDEXES,

    /**
     * Treat a token as a column name no matter what (even if it looks like it is an integer). This
     * is used by the Web UI, which blindly specifies column names.
     */
    NAMES_ONLY
  }

  public class MultiVecSelect extends Dependent {
    boolean _namesOnly;

    private void init(MultiVecSelectType selectType) {
      _namesOnly = false;
      switch( selectType ) {
        case NAMES_THEN_INDEXES:
          _namesOnly = false;
          break;

        case NAMES_ONLY:
          _namesOnly = true;
          break;
      }
    }

    protected MultiVecSelect(String key) {
      super(key);
      init(MultiVecSelectType.NAMES_THEN_INDEXES);
    }

    protected MultiVecSelect(String key, MultiVecSelectType selectType) {
      super(key);
      init(selectType);
    }
  }

  public class DoClassBoolean extends Dependent {
    protected DoClassBoolean(String key) {
      super(key);
    }
  }
  public class DRFCopyDataBoolean extends Dependent {
    protected DRFCopyDataBoolean(String key) { super(key); }
  }

  /**
   * Iterates over fields and their annotations, and creates argument handlers.
   */
  @Override protected void registered(API_VERSION version) {
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

      // TODO remove map, response field already processed specifically
      HashMap<String, FrameClassVec> classVecs = new HashMap<String, FrameClassVec>();
      for( Field f : fields ) {
        Annotation[] as = f.getAnnotations();
        API api = find(as, API.class);

        if( api != null && Helper.isInput(api) ) {
          f.setAccessible(true);
          Object defaultValue = f.get(this);

          // Create an Argument instance to reuse existing Web framework for now
          Argument arg = null;

          // Simplest case, filter is an Argument
          if( Argument.class.isAssignableFrom(api.filter()) ) {
            arg = (Argument) newInstance(api);
          }

          //
          else if( ColumnSelect.class.isAssignableFrom(api.filter()) ) {
            ColumnSelect name = (ColumnSelect) newInstance(api);
            throw H2O.fail();
            //H2OHexKey key = null;
            //for( Argument a : _arguments )
            //  if( a instanceof H2OHexKey && name._ref.equals(((H2OHexKey) a)._name) )
            //    key = (H2OHexKey) a;
            //arg = new HexAllColumnSelect(f.getName(), key);
          }

          //
          else if( Dependent.class.isAssignableFrom(api.filter()) ) {
            Dependent d = (Dependent) newInstance(api);
            Argument ref = find(d._ref);
            if( d instanceof VecSelect )
              arg = new FrameKeyVec(f.getName(), (TypeaheadKey) ref, api.help(), api.required());
            else if( d instanceof VecClassSelect ) {
              arg = new FrameClassVec(f.getName(), (TypeaheadKey) ref);
              classVecs.put(d._ref, (FrameClassVec) arg);
            } else if( d instanceof MultiVecSelect ) {
              FrameClassVec response = classVecs.get(d._ref);
              boolean names = ((MultiVecSelect) d)._namesOnly;
              arg = new FrameKeyMultiVec(f.getName(), (TypeaheadKey) ref, response, api.help(), names,filterNaCols());
            } else if( d instanceof DoClassBoolean ) {
              FrameClassVec response = classVecs.get(d._ref);
              arg = new ClassifyBool(f.getName(), response);
            } else if( d instanceof DRFCopyDataBoolean ) {
              arg = new DRFCopyDataBool(f.getName(), (TypeaheadKey)ref);
            }
          }
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

          // RSeq
          else if( f.getType() == int[].class ) {
            int[] val = (int[]) defaultValue;
            double[] ds = null;
            if( val != null ) {
              ds = new double[val.length];
              for( int i = 0; i < ds.length; i++ )
                ds[i] = val[i];
            }
            arg = new RSeq(f.getName(), api.required(), new NumberSequence(ds, null, true), false, api.help());
          }

          // RSeq
          else if( f.getType() == double[].class ) {
            double[] val = (double[]) defaultValue;
            arg = new RSeq(f.getName(), api.required(), new NumberSequence(val, null, false), false, api.help());
          }

          // RSeq float
          else if( f.getType() == float[].class ) {
            float[] val = (float[]) defaultValue;
            arg = new RSeqFloat(f.getName(), api.required(), new NumberSequenceFloat(val, null, false), false, api.help());
          }

          // Bool
          else if( f.getType() == boolean.class && api.filter() == Default.class ) {
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

          // Generic Freezable field
          else if( Freezable.class.isAssignableFrom(f.getType()) )
            arg = new TypeaheadKey(f.getType(), api.required());

          if( arg != null ) {
            arg._name = f.getName();
            arg._displayName = api.displayName().length() > 0 ? api.displayName() : null;
            arg._required = api.required();
            arg._field = f;
            arg._hideInQuery = api.hide();
            arg._gridable = api.gridable();
            arg._mustExist = api.mustExist();
            arg._validator = newValidator(api);
          }
        }
      }
    } catch( Exception e ) {
      throw new RuntimeException(e);
    }
  }

  final protected Argument find(String name) {
    for( Argument a : _arguments )
      if( name.equals(a._name) )
        return a;
    return null;
  }

  // Extracted in separate class as Weaver cannot load Request during boot
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

  private Validator newValidator(API api) throws Exception {
    for( Constructor c : api.validator().getDeclaredConstructors() ) {
      c.setAccessible(true);
      Class[] ps = c.getParameterTypes();
      return (Validator) c.newInstance();
    }
    return null;
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

  public Response servePublic() {
    return serve();
  }

  // Expand grid search related argument sets
  @Override protected NanoHTTPD.Response serveGrid(NanoHTTPD server, Properties parms, RequestType type) {
    String[][] values = new String[_arguments.size()][];
    boolean gridSearch = false;
    for( int i = 0; i < _arguments.size(); i++ ) {
      Argument arg = _arguments.get(i);
      if( arg._gridable ) {
        String value = _parms.getProperty(arg._name);
        if( value != null ) {
          // Skips grid if argument is an array, except if imbricated expression
          // Little hackish, waiting for real language
          boolean imbricated = value.contains("(");
          if( !arg._field.getType().isArray() || imbricated ) {
            values[i] = split(value);
            if( values[i] != null && values[i].length > 1 )
              gridSearch = true;
          } else if (arg._field.getType().isArray() && !imbricated) { // Copy values which are arrays
            values[i] = new String[] { value };
          }
        }
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

  // Splits one-level imbricated expressions like 4, 5, (2, 3), 7
  // TODO: switch to real parser for unified imbricated argument sets, expressions etc.
  public static String[] split(String value) {
    String[] values = null;
    value = value.trim();
    StringTokenizer st = new StringTokenizer(value, ",()", true);
    String s, current = "";
    while( (s = getNextToken(st)) != null ) {
      if( ",".equals(s) ) {
        values = addSplit(values, current);
        current = "";
      } else if( "(".equals(s) ) {
        while( !(")".equals((s = getNextToken(st)))) ) {
          if( s == null )
            throw new IllegalArgumentException("Missing closing parenthesis");
          current += s;
        }
        values = addSplit(values, current);
        current = "";
      } else
        current += s;
    }
    values = addSplit(values, current);
    return values;
  }

  private static String[] addSplit(String[] values, String value) {
    if( value.contains(":") ) {
      double[] gen = NumberSequence.parseGenerator(value, false, 1);
      for( double d : gen )
        values = Utils.append(values, "" + d);
    } else if( value.length() > 0 )
      values = Utils.append(values, value);
    return values;
  }

  private static String getNextToken(StringTokenizer st) {
    while( st.hasMoreTokens() ) {
      String tok = st.nextToken().trim();
      if( tok.length() > 0 )
        return tok;
    }
    return null;
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

  public void set(Argument arg, String input, Object value) {
    if( arg._field.getType() != Key.class && value instanceof Key )
      value = UKV.get((Key) value);

    try {
      //
      if( arg._field.getType() == int.class && value instanceof Long )
        value = ((Long) value).intValue();
      //
      else if( arg._field.getType() == float.class && value instanceof Double )
        value = ((Double) value).floatValue();
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
      else if( value instanceof NumberSequenceFloat ) {
        float[] fs = ((NumberSequenceFloat) value)._arr;
        if( arg._field.getType() == int[].class ) {
          int[] is = new int[fs.length];
          for( int i = 0; i < is.length; i++ )
            is[i] = (int) fs[i];
          value = is;
        } else
          value = fs;
      }
      arg._field.set(this, value);
    } catch( Exception e ) {
      throw new RuntimeException(e);
    }
  }

  @Override public API_VERSION[] supportedVersions() {
    return SUPPORTS_ONLY_V2;
  }

  public void fillResponseInfo(Response response) {
    this.response_info = response.extractInfo();
  }

  public JsonObject toJSON() {
    final String json = new String(writeJSON(new AutoBuffer()).buf());
    if (json.length() == 0) return new JsonObject();
    JsonObject jo = (JsonObject)new JsonParser().parse(json);
    jo.remove("Request2");
    jo.remove("response_info");
    return jo;
  }

  public JsonObject toJSON(Set<String> whitelist) {
    JsonObject jo = toJSON();

    for (Map.Entry<String , JsonElement> entry : jo.entrySet()) {
      String key = entry.getKey();
      if (! whitelist.contains(key))
        jo.remove(key);
    }
    return jo;
  }

  @Override
  public String toString() {
    return GSON_BUILDER.toJson(toJSON());
  }

  protected void logStart() {
    Log.info("Building H2O " + this.getClass().getSimpleName() + " model with these parameters:");
    for (String s : toString().split("\n")) Log.info(s);
  }

  public boolean makeJsonBox(StringBuilder sb) {
    sb.append("<div class='pull-right'><a href='#' onclick='$(\"#params\").toggleClass(\"hide\");'"
            + " class='btn btn-inverse btn-mini'>Model Parameters</a></div><div class='hide' id='params'>"
            + "<pre><code class=\"language-json\">");
    sb.append(toString());
    sb.append("</code></pre></div>");
    return true;
  }
  protected boolean filterNaCols(){return false;}
}
