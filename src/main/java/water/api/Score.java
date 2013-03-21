package water.api;

import java.util.Properties;

import water.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Simple web page to guide model validation.
 */
public class Score extends Request {
  protected final H2OModelKey<Model,TypeaheadModelKeyRequest> _modelKey;

  public Score() {
    _modelKey = new H2OModelKey(new TypeaheadModelKeyRequest(),MODEL_KEY,true);
    // Force refresh because the true set of argument queries depends on the
    // columns in the model... when it finally appears.
    _modelKey.setRefreshOnChange();
  }

  // Each 'Score' query/request is unique, and reflects the Model that is
  // initially fed into it.  Really it should reflect the ValueArray...  so
  // this mechanism can be used to hand-roll new rows.
  public static Score create( Properties parms ) {
    Score S = new Score();
    String mstr = parms.getProperty(MODEL_KEY);
    if( mstr == null ) return S;
    Model M = null;
    try { M = S._modelKey.parse(mstr); }
    catch( IllegalArgumentException iae ) { return S; }

    // Create Argument queries for each column.  No real argument checking is
    // possible, because the data has no semantic content to H2O.  It's all
    // just numbers.  In particular, we do print the ranges but do not
    // range-check, in case the user WANTS to observe a value outside the
    // observed range.
    for( int i=0; i<M._va._cols.length-1; i++ ) {
      ValueArray.Column C = M._va._cols[i];
      String name = Str2JSON(C._name);
      if( C._domain != null ) {
        S.new FactorSelect(name,true,C._domain,(int)C._mean);
      } else if( C.isFloat() ) { // Add a float column
        S.new Real   (name,      C._mean, "range: "+C._min+" - "+C._max);
      } else {                  // Add an integer column
        S.new LongInt(name,(long)C._mean, "range: "+C._min+" - "+C._max);
      }
    }

    return S;
  }


  @Override protected Response serve() {
    // The Model being scored
    Model M = _modelKey.value();
    // Extract the datarow from the argument vector.
    // The args are in the same order as the columns, except for the leading model_key.
    double d[] = new double[M._va._cols.length-1];
    for( int i=0; i<d.length; i++ ) {
      Argument arg = _arguments.get(i+1);
      if( false ) ;
      else if( arg instanceof FactorSelect ) d[i] = (Integer)arg.value();
      else if( arg instanceof Real         ) d[i] = (Double )arg.value();
      else if( arg instanceof LongInt      ) d[i] = (Long   )arg.value();
      else throw H2O.unimpl();
    }

    // Response json
    JsonObject res = new JsonObject();
    res.addProperty(MODEL_KEY, M._selfKey.toString());

    // Make a single row in 'rows' in the json
    JsonArray rows = new JsonArray();
    JsonObject obj = new JsonObject();
    obj.addProperty(ROW,0);     // Bogus row number
    for( int i=0; i<d.length; i++ ) {
      ValueArray.Column C = M._va._cols[i];
      obj.addProperty(C._name,
                      C._domain==null
                      ? (C.isFloat() ? Double.toString(d[i]) : Long.toString((long)d[i]))
                      : C._domain[(int)d[i]]);
    }
    rows.add(obj);
    res.add(ROWS, rows);

    // Score the row on the model.  May destroy 'd'.
    double response = M.score(d,null);
    res.addProperty(CLASS, response);

    // Display HTML setup
    Response r = Response.done(res);
    return r;
  }

  // Pick which factor/enum/category by string
  private class FactorSelect extends InputSelect<Integer> {
    public final String[] _ss;
    public final int _default;
    public FactorSelect( String name, boolean req, String[] ss, int def ) {
      super(name,req);
      _ss = ss;
      _default = def;
    }
    @Override protected String[] selectValues() { return _ss; }
    @Override protected String selectedItemValue() {
      Integer ii = value();
      return _ss[ii==null ? 0 : ii];
    }
    @Override protected Integer parse(String input) throws IllegalArgumentException {
      for( int i=0; i<_ss.length; i++ )
        if( _ss[i].equals(input) )
          return i;
      throw new IllegalArgumentException(input+" is not a factor in the column");
    }
    @Override protected Integer defaultValue() { return _default; }
    @Override protected String queryDescription() { return "Select a factor from the column"; }
  }
}
