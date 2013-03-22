package water.api;

import com.google.gson.*;

import hex.DGLM.GLMModel;
import hex.GLMGrid.GLMModels;
import hex.KMeans.KMeansModel;
import hex.rf.RFModel;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.zip.*;

import water.*;
import water.ValueArray.Column;
import water.api.GLM.GLMBuilder;
import water.parser.CsvParser;
import water.util.Utils;

public class Inspect extends Request {
  private static final HashMap<String, String> _displayNames = new HashMap<String, String>();
  private static final long                    INFO_PAGE     = -1;
  private final H2OExistingKey                 _key          = new H2OExistingKey(KEY);
  private final LongInt                        _offset       = new LongInt(OFFSET, 0L, INFO_PAGE, Long.MAX_VALUE, "");
  private final Int                            _view         = new Int(VIEW, 100, 0, 10000);

  static {
    _displayNames.put(ENUM_DOMAIN_SIZE, "Enum Domain");
    _displayNames.put(MEAN, "&mu;");
    _displayNames.put(NUM_MISSING_VALUES, "Missing");
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
    if( val.type() == TypeMap.PRIM_B )
      return serveUnparsedValue(val);
    Freezable f = val.getFreezable();
    if( f instanceof ValueArray ) {
      ValueArray ary = (ValueArray)f;
      if( ary._cols.length==1 && ary._cols[0]._name==null )
        return serveUnparsedValue(val);
      return serveValueArray(ary);
    }
    if( f instanceof GLMModel ) {
      GLMModel m = (GLMModel)f;
      JsonObject res = new JsonObject();
      res.add(GLMModel.NAME, m.toJson());
      Response r = Response.done(res);
      r.setBuilder(ROOT_OBJECT, new GLMBuilder(m));
      return r;
    }
    if( f instanceof hex.GLMGrid.GLMModels ) {
      JsonObject resp = new JsonObject();
      resp.addProperty(Constants.DEST_KEY, val._key.toString());
      return GLMGridProgress.redirect(resp,null,val._key);
    }

    if( f instanceof KMeansModel ) {
      KMeansModel m = (KMeansModel)f;
      JsonObject res = new JsonObject();
      res.add(KMeansModel.NAME, m.toJson());
      Response r = Response.done(res);
      r.setBuilder(KMeansModel.NAME, new KMeans.Builder(m));
      return r;
    }
    if( f instanceof RFModel ) {
      JsonObject res = new JsonObject();
      return RFView.redirect(res,val._key);
    }
    if( f instanceof Job.Fail ) {
      UKV.remove(val._key);   // Not sure if this is a good place to do this
      return Response.error(((Job.Fail)f)._message);
    }
    return Response.error("No idea how to display a "+f.getClass());
  }

  // Look at unparsed data; guess its setup
  public static CsvParser.Setup csvGuessValue(Value v) {
    // See if we can make sense of the first few rows.
    byte[] bs = v.getFirstBytes(); // Read some bytes
    int off = 0;
    // First decrypt compression
    InputStream is = null;
    try {
      switch( water.parser.ParseDataset.guessCompressionMethod(v) ) {
      case NONE: // No compression
        off = bs.length; // All bytes ready already
        break;
      case ZIP: {
        ZipInputStream zis = new ZipInputStream(v.openStream());
        ZipEntry ze = zis.getNextEntry(); // Get the *FIRST* entry
        // There is at least one entry in zip file and it is not a directory.
        if( ze != null || !ze.isDirectory() )
          is = zis;
        break;
      }
      case GZIP:
        is = new GZIPInputStream(v.openStream());
        break;
      }
      // If reading from a compressed stream, estimate we can read 2x uncompressed
      if( is != null )
        bs = new byte[bs.length * 2];
      // Now read from the (possibly compressed) stream
      while( off < bs.length ) {
        int len = is.read(bs, off, bs.length - off);
        if( len < 0 )
          break;
        off += len;
        if( off == bs.length ) { // Dataset is uncompressing alot! Need more space...
          if( bs.length >= ValueArray.CHUNK_SZ )
            break; // Already got enough
          bs = Arrays.copyOf(bs, bs.length * 2);
        }
      }
    } catch( IOException ioe ) { // Stop at any io error
    } finally {
      Utils.close(is);
    }
    if( off < bs.length )
      bs = Arrays.copyOf(bs, off); // Trim array to length read

    // Now try to interpret the unzipped data as a CSV
    return CsvParser.inspect(bs);
  }

  // Build a response JSON
  private final Response serveUnparsedValue(Value v) {
    JsonObject result = new JsonObject();
    result.addProperty(VALUE_TYPE, "unparsed");

    CsvParser.Setup setup = csvGuessValue(v);
    if( setup._data != null && setup._data[1].length > 0 ) { // Able to parse sanely?
      int zipped_len = v.getFirstBytes().length;
      double bytes_per_row = (double) zipped_len / setup._numlines;
      long rows = (long) (v.length() / bytes_per_row);
      result.addProperty(NUM_ROWS, "~" + rows); // approx rows
      result.addProperty(NUM_COLS, setup._data[1].length);
      result.add(ROWS, new Gson().toJsonTree(setup._data));
    } else {
      result.addProperty(NUM_ROWS, "unknown");
      result.addProperty(NUM_COLS, "unknown");
    }
    result.addProperty(VALUE_SIZE, v.length());

    // The builder Response
    Response r = Response.done(result);
    // Some nice links in the response
    r.addHeader("<div class='alert'>" //
        + Parse.link(v._key, "Parse into hex format") + " or " //
        + RReader.link(v._key, "from R data") + " </div>");
    // Set the builder for showing the rows
    r.setBuilder(ROWS, new ArrayBuilder() {
      public String caption(JsonArray array, String name) {
        return "<h4>First few sample rows</h4>";
      }
    });

    return r;
  }

  public Response serveValueArray(final ValueArray va) {
    if( _offset.value() > va._numrows )
      return Response.error("Value only has " + va._numrows + " rows");

    JsonObject result = new JsonObject();
    result.addProperty(VALUE_TYPE, "parsed");
    result.addProperty(KEY, va._key.toString());
    result.addProperty(NUM_ROWS, va._numrows);
    result.addProperty(NUM_COLS, va._cols.length);
    result.addProperty(ROW_SIZE, va._rowsize);
    result.addProperty(VALUE_SIZE, va.length());

    JsonArray cols = new JsonArray();
    JsonArray rows = new JsonArray();

    for( int i = 0; i < va._cols.length; i++ ) {
      Column c = va._cols[i];
      JsonObject json = new JsonObject();
      json.addProperty(NAME, c._name);
      json.addProperty(OFFSET, (int) c._off);
      json.addProperty(SIZE, Math.abs(c._size));
      json.addProperty(BASE, c._base);
      json.addProperty(SCALE, (int) c._scale);
      json.addProperty(MIN, c._min);
      json.addProperty(MAX, c._max);
      json.addProperty(MEAN, c._mean);
      json.addProperty(VARIANCE, c._sigma);
      json.addProperty(NUM_MISSING_VALUES, va._numrows - c._n);
      json.addProperty(TYPE, c._domain != null ? "enum" : (c.isFloat() ? "float" : "int"));
      json.addProperty(ENUM_DOMAIN_SIZE, c._domain != null ? c._domain.length : 0);
      cols.add(json);
    }

    if( _offset.value() != INFO_PAGE ) {
      long endRow = Math.min(_offset.value() + _view.value(), va._numrows);
      long startRow = Math.min(_offset.value(), va._numrows - _view.value());
      for( long row = Math.max(0, startRow); row < endRow; ++row ) {
        JsonObject obj = new JsonObject();
        obj.addProperty(ROW, row);
        for( int i = 0; i < va._cols.length; ++i )
          format(obj, va, row, i);
        rows.add(obj);
      }
    }

    result.add(COLS, cols);
    result.add(ROWS, rows);

    Response r = Response.done(result);
    r.setBuilder(ROOT_OBJECT, new ObjectBuilder() {
      @Override
      public String build(Response response, JsonObject object, String contextName) {
        String s = html(va);
        Table t = new Table(argumentsToJson(), _offset.value(), _view.value(), va);
        s += t.build(response, object.get(ROWS), ROWS);
        return s;
      }
    });
    r.setBuilder(ROWS + "." + ROW, new ArrayRowElementBuilder() {
      @Override
      public String elementToString(JsonElement elm, String contextName) {
        String json = elm.getAsString();
        String html = _displayNames.get(json);
        return html != null ? html : RequestStatics.JSON2HTML(json);
      }
    });
    return r;
  }

  private static void format(JsonObject obj, ValueArray va, long rowIdx, int colIdx) {
    if( rowIdx < 0 || rowIdx >= va._numrows )
      return;
    if( colIdx >= va._cols.length )
      return;
    ValueArray.Column c = va._cols[colIdx];
    String name = c._name != null ? c._name : "" + colIdx;
    if( va.isNA(rowIdx, colIdx) ) {
      obj.addProperty(name, "NA");
    } else if( c._domain != null ) {
      obj.addProperty(name, c._domain[(int) va.data(rowIdx, colIdx)]);
    } else if( (c._size > 0) && (c._scale == 1) ) {
      obj.addProperty(name, va.data(rowIdx, colIdx));
    } else {
      obj.addProperty(name, va.datad(rowIdx, colIdx));
    }
  }

  private final String html(ValueArray ary) {
    String keyParam = KEY + "=" + ary._key.toString();
    StringBuilder sb = new StringBuilder();
    // @formatter:off
    sb.append(""
        + "<h3>"
          + "<a href='RemoveAck.html?" + keyParam + "'>"
          + "<button class='btn btn-danger btn-mini'>X</button></a>"
          + "&nbsp;&nbsp;" + ary._key.toString()
        + "</h3>"
        + "<div class='alert'>" + "Build models using "
          + RF.link(ary._key, "Random Forest") + ", "
          + GLM.link(ary._key, "GLM") + ", " + GLMGrid.link(ary._key, "GLM Grid Search") + ", or "
          + KMeans.link(ary._key, "KMeans") + "<br />"
          + "Score data using "
          + RFScore.link(ary._key, "Random Forest") + "."
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
      if( array.size() == 0 ) { // Fake row, needed by builder
        array = new JsonArray();
        JsonObject fake = new JsonObject();
        fake.addProperty(ROW, 0);
        for( int i = 0; i < _va._cols.length; ++i )
          format(fake, _va, 0, i);
        array.add(fake);
      }
      sb.append(header(array));

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

      row.addProperty(ROW, NUM_MISSING_VALUES);
      for( int i = 0; i < _va._cols.length; i++ )
        row.addProperty(_va._cols[i]._name, _va._numrows - _va._cols[i]._n);
      sb.append(defaultBuilder(row).build(response, row, contextName));

      if( _offset == INFO_PAGE ) {
        row.addProperty(ROW, OFFSET);
        for( int i = 0; i < _va._cols.length; i++ )
          row.addProperty(_va._cols[i]._name, (int) _va._cols[i]._off);
        sb.append(defaultBuilder(row).build(response, row, contextName));

        row.addProperty(ROW, SIZE);
        for( int i = 0; i < _va._cols.length; i++ )
          row.addProperty(_va._cols[i]._name, Math.abs(_va._cols[i]._size));
        sb.append(defaultBuilder(row).build(response, row, contextName));

        row.addProperty(ROW, BASE);
        for( int i = 0; i < _va._cols.length; i++ )
          row.addProperty(_va._cols[i]._name, _va._cols[i]._base);
        sb.append(defaultBuilder(row).build(response, row, contextName));

        row.addProperty(ROW, SCALE);
        for( int i = 0; i < _va._cols.length; i++ )
          row.addProperty(_va._cols[i]._name, (int) _va._cols[i]._scale);
        sb.append(defaultBuilder(row).build(response, row, contextName));

        row.addProperty(ROW, ENUM_DOMAIN_SIZE);
        for( int i = 0; i < _va._cols.length; i++ )
          row.addProperty(_va._cols[i]._name, _va._cols[i]._domain != null ? _va._cols[i]._domain.length : 0);
        sb.append(defaultBuilder(row).build(response, row, contextName));
      } else {
        for( JsonElement e : array ) {
          Builder builder = response.getBuilderFor(contextName + "_ROW");
          if( builder == null )
            builder = defaultBuilder(e);
          sb.append(builder.build(response, e, contextName));
        }
      }

      sb.append(footer(array));
      return sb.toString();
    }
  }
}
