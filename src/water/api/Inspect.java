package water.api;

import hex.GLMSolver.GLMModel;
import hex.KMeans.KMeansModel;

import java.util.HashMap;

import water.*;
import water.api.GLM.GLMBuilder;
import water.parser.CsvParser;

import com.google.gson.*;

public class Inspect extends Request {
  private static final HashMap<String, String> _displayNames = new HashMap<String, String>();
  private static final long                    INFO_PAGE     = -1;

  private final H2OExistingKey                 _key          = new H2OExistingKey(KEY);
  private final LongInt                        _offset       = new LongInt(OFFSET, 0L, INFO_PAGE, Long.MAX_VALUE, "");
  private final Int                            _view         = new Int(VIEW, 100, 0, 10000);

  static {
    _displayNames.put(OFFSET, "Offset");
    _displayNames.put(SIZE, "Size");
    _displayNames.put(BASE, "Base");
    _displayNames.put(SCALE, "Scale");
    _displayNames.put(MIN, "Min");
    _displayNames.put(MAX, "Max");
    _displayNames.put(BADAT, "Bad");
    _displayNames.put(MEAN, "&mu;");
    _displayNames.put(VARIANCE, "&sigma;");
  }

  // Constructor called from 'Exec' query instead of the direct view links
  Inspect(Key k) {
    _key.reset();
    _key.check(k.toString());
    _offset.reset();
    _offset.check("");
    _view.reset();
    _view.check("");
  }

  // Default no-args constructor
  Inspect() {
  }

  public static Response redirect(JsonObject resp, Key dest) {
    JsonObject redir = new JsonObject();
    redir.addProperty(KEY, dest.toString());
    return Response.redirect(resp, Inspect.class, redir);
  }

  @Override
  protected Response serve() {
    Value val = _key.value();
    if( val.isHex() ) {
      return serveValueArray(ValueArray.value(val));
    }
    if( _key.originalValue().startsWith(GLMModel.KEY_PREFIX) ) {
      GLMModel m = val.get(new GLMModel());
      JsonObject res = new JsonObject();
      // Convert to JSON
      res.add("GLMModel", m.toJson());
      // Display HTML setup
      Response r = Response.done(res);
      r.setBuilder(""/* top-level do-it-all builder */, new GLMBuilder(m));
      return r;
    }
    if( _key.originalValue().startsWith(KMeansModel.KEY_PREFIX) ) {
      KMeansModel m = new KMeansModel().read(new AutoBuffer(val.get(), 0));
      JsonObject res = new JsonObject();
      // Convert to JSON
      res.add("KMeansModel", m.toJson());
      // Display HTML setup
      Response r = Response.done(res);
      // r.setBuilder(""/*top-level do-it-all builder*/,new KMeansBuilder(m));
      return r;
    }

    return serveUnparsedValue(val);
  }

  private final Response serveUnparsedValue(Value v) {
    JsonObject result = new JsonObject();
    result.addProperty(VALUE_TYPE, "unparsed");

    byte[] bs = v.getFirstBytes();
    int[] rows_cols = CsvParser.inspect(bs);
    if( rows_cols != null && rows_cols[1] != 0 ) { // Able to parse sanely?
      double bytes_per_row = (double) bs.length / rows_cols[0];
      long rows = (long) (v.length() / bytes_per_row);
      result.addProperty(ROWS, "~" + rows); // approx rows
      result.addProperty(COLS, rows_cols[1]);
    } else {
      result.addProperty(ROWS, "unknown");
      result.addProperty(COLS, "unknown");
    }
    result.addProperty(VALUE_SIZE, v.length());

    Response r = Response.done(result);
    r.addHeader("<div class='alert'>" //
        + Parse.link(v._key, "Parse into hex format") + ", or " //
        + RReader.link(v._key, "from R data") + " </div>");
    return r;
  }

  public Response serveValueArray(final ValueArray va) {
    JsonObject result = new JsonObject();
    result.addProperty(VALUE_TYPE, "parsed");
    result.addProperty(KEY, va._key.toString());
    result.addProperty(NUM_ROWS, va._numrows);
    result.addProperty(NUM_COLS, va._cols.length);
    result.addProperty(ROW_SIZE, va._rowsize);
    result.addProperty(VALUE_SIZE, va.length());

    if( _offset.value() == INFO_PAGE ) {
      JsonArray rows = new JsonArray();
      JsonObject row;

      rows.add(row = new JsonObject());
      row.addProperty(ROW, OFFSET);
      for( int i = 0; i < va._cols.length; i++ )
        row.addProperty(va._cols[i]._name, (int) va._cols[i]._off);

      rows.add(row = new JsonObject());
      row.addProperty(ROW, SIZE);
      for( int i = 0; i < va._cols.length; i++ )
        row.addProperty(va._cols[i]._name, Math.abs(va._cols[i]._size));

      rows.add(row = new JsonObject());
      row.addProperty(ROW, BASE);
      for( int i = 0; i < va._cols.length; i++ )
        row.addProperty(va._cols[i]._name, va._cols[i]._base);

      rows.add(row = new JsonObject());
      row.addProperty(ROW, SCALE);
      for( int i = 0; i < va._cols.length; i++ )
        row.addProperty(va._cols[i]._name, (int) va._cols[i]._scale);

      rows.add(row = new JsonObject());
      row.addProperty(ROW, MIN);
      for( int i = 0; i < va._cols.length; i++ )
        row.addProperty(va._cols[i]._name, va._cols[i]._min);

      rows.add(row = new JsonObject());
      row.addProperty(ROW, MAX);
      for( int i = 0; i < va._cols.length; i++ )
        row.addProperty(va._cols[i]._name, va._cols[i]._max);

      rows.add(row = new JsonObject());
      row.addProperty(ROW, MEAN);
      for( int i = 0; i < va._cols.length; i++ )
        row.addProperty(va._cols[i]._name, va._cols[i]._mean);

      rows.add(row = new JsonObject());
      row.addProperty(ROW, VARIANCE);
      for( int i = 0; i < va._cols.length; i++ )
        row.addProperty(va._cols[i]._name, va._cols[i]._sigma);

      rows.add(row = new JsonObject());
      row.addProperty(ROW, BADAT);
      for( int i = 0; i < va._cols.length; i++ )
        row.addProperty(va._cols[i]._name, va._numrows - va._cols[i]._n);

      result.add(ROW_DATA, rows);
    } else {
      if( _offset.value() > va._numrows )
        return Response.error("Value only has " + va._numrows + " rows");
      JsonArray rows = new JsonArray();
      long endRow = Math.min(_offset.value() + _view.value(), va._numrows);
      long startRow = Math.min(_offset.value(), va._numrows - _view.value());
      for( long row = Math.max(0, startRow); row < endRow; ++row ) {
        JsonObject obj = new JsonObject();
        obj.addProperty(ROW, row);
        for( int i = 0; i < va._cols.length; ++i )
          format(obj, va, row, i);
        rows.add(obj);
      }
      result.add(ROW_DATA, rows);
    }

    Response r = Response.done(result);
    r.setBuilder(ROOT_OBJECT, new ObjectBuilder() {
      @Override
      public String build(Response response, JsonObject object, String contextName) {
        String s = html(va);
        Table t = new Table(argumentsToJson(), _offset.value(), _view.value(), va);
        s += t.build(response, object.get(ROW_DATA), ROW_DATA);
        return s;
      }
    });
    r.setBuilder(ROW_DATA + "." + ROW, new ArrayRowElementBuilder() {
      @Override
      public String elementToString(JsonElement elm, String contextName) {
        String s = _displayNames.get(elm.getAsString());
        return s != null ? s : super.elementToString(elm, contextName);
      }
    });
    return r;
  }

  private final void format(JsonObject obj, ValueArray va, long rowIdx, int colIdx) {
    if( rowIdx < 0 || rowIdx >= va._numrows )
      return;
    if( colIdx >= va._cols.length )
      return;
    ValueArray.Column c = va._cols[colIdx];
    if( va.isNA(rowIdx, colIdx) ) {
      obj.addProperty(c._name, "NA");
    } else if( c._domain != null ) {
      obj.addProperty(c._name, c._domain[(int) va.data(rowIdx, colIdx)]);
    } else if( (c._size > 0) && (c._scale == 1) ) {
      obj.addProperty(c._name, va.data(rowIdx, colIdx));
    } else {
      obj.addProperty(c._name, va.datad(rowIdx, colIdx));
    }
  }

  private final String html(ValueArray ary) {
    String keyParam = KEY + "=" + ary._key.toString();
    StringBuilder sb = new StringBuilder();
    // @formatter:off
    sb.append(""
        + "<h3>"
          + "<a style='%delBtnStyle' href='RemoveAck.html?" + keyParam + "'>"
          + "<button class='btn btn-danger btn-mini'>X</button></a>"
          + "&nbsp;&nbsp;" + ary._key.toString()
        + "</h3>"
        + "<div class='alert'>" + "Build models using "
          + RF.link(ary._key, "Random Forest") + ", "
          + GLM.link(ary._key, "GLM") + ", " + GLMGrid.link(ary._key, "GLM Grid Search") + ", or "
          + KMeans.link(ary._key, "KMeans")
        + "</div>"
        + "<p><b><font size=+1>"
          + ary._cols.length + " columns, "
          + ary._rowsize + " bytes-per-row * " + ary._numrows + " rows = " + PrettyPrint.bytes(ary.length())
        + "</font></b></p>");
    // @formatter:on
    return sb.toString();
  }

  private static final class Table extends PaginatedTable {
    private final ValueArray _va;

    public Table(JsonObject query, long offset, int view, ValueArray va) {
      super(query, offset, view, va._numrows, true);
      _va = va;
    }

    @Override
    public String build(Response response, JsonArray array, String contextName) {
      StringBuilder sb = new StringBuilder();
      sb.append(header(array));

      if( _offset != INFO_PAGE ) {
        JsonObject row = new JsonObject();

        row.addProperty(ROW, MIN);
        for( int i = 0; i < _va._cols.length; i++ )
          row.addProperty(_va._cols[i]._name, _va._cols[i]._min);
        sb.append(defaultBuilder(row).build(response, row, contextName));

        row.addProperty(ROW, MAX);
        for( int i = 0; i < _va._cols.length; i++ )
          row.addProperty(_va._cols[i]._name, _va._cols[i]._max);
        sb.append(defaultBuilder(row).build(response, row, contextName));

        row.addProperty(ROW, MEAN);
        for( int i = 0; i < _va._cols.length; i++ )
          row.addProperty(_va._cols[i]._name, _va._cols[i]._mean);
        sb.append(defaultBuilder(row).build(response, row, contextName));

        row.addProperty(ROW, VARIANCE);
        for( int i = 0; i < _va._cols.length; i++ )
          row.addProperty(_va._cols[i]._name, _va._cols[i]._sigma);
        sb.append(defaultBuilder(row).build(response, row, contextName));

        row.addProperty(ROW, BADAT);
        for( int i = 0; i < _va._cols.length; i++ )
          row.addProperty(_va._cols[i]._name, _va._numrows - _va._cols[i]._n);
        sb.append(defaultBuilder(row).build(response, row, contextName));
      }

      for( JsonElement e : array ) {
        Builder builder = response.getBuilderFor(contextName + "_ROW");
        if( builder == null )
          builder = defaultBuilder(e);
        sb.append(builder.build(response, e, contextName));
      }

      sb.append(footer(array));
      return sb.toString();
    }
  }
}
