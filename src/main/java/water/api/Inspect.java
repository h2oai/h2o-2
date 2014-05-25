package water.api;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import hex.DGLM.GLMModel;
import hex.GridSearch;
import hex.KMeans2.KMeans2Model;
import hex.KMeans2.KMeans2ModelView;
import hex.KMeansModel;
import hex.NeuralNet.NeuralNetModel;
import hex.deeplearning.DeepLearning;
import hex.deeplearning.DeepLearningModel;
import hex.drf.DRF;
import hex.drf.DRF.DRFModel;
import hex.gapstat.GapStatisticModel;
import hex.gapstat.GapStatisticModelView;
import hex.gbm.GBM;
import hex.gbm.GBM.GBMModel;
import hex.glm.GLM2;
import hex.glm.GLMModelView;
import hex.nb.NBModelView;
import hex.pca.PCA;
import hex.pca.PCAModelView;
import hex.rf.RFModel;
import hex.singlenoderf.SpeeDRFModel;
import hex.singlenoderf.SpeeDRFModelView;
import water.*;
import water.ValueArray.Column;
import water.api.GLMProgressPage.GLMBuilder;
import water.api.Inspect2.ColSummary.ColType;
import water.fvec.Frame;
import water.fvec.Vec;
import water.parser.CustomParser.PSetupGuess;
import water.parser.ParseDataset;
import water.util.Utils;

import java.util.HashMap;

public class Inspect extends Request {
  private static final HashMap<String, String> _displayNames = new HashMap<String, String>();
  private static final long                    INFO_PAGE     = -1;
  private final H2OExistingKey                 _key          = new H2OExistingKey(KEY);
  private final LongInt                        _offset       = new LongInt(OFFSET, INFO_PAGE, Long.MAX_VALUE);
  private final Int                            _view         = new Int(VIEW, 100, 0, 10000);
  private final Str                            _producer     = new Str(JOB, null);
  private final Int                            _max_column   = new Int(COLUMNS_DISPLAY, MAX_COLUMNS_TO_DISPLAY);

  static final int MAX_COLUMNS_TO_DISPLAY = 1000;
  static final String NA = ""; // not available information

  static {
    _displayNames.put(ENUM_DOMAIN_SIZE, "Enum Domain");
    _displayNames.put(MEAN, "avg");
    _displayNames.put(NUM_MISSING_VALUES, "Missing");
    _displayNames.put(VARIANCE, "sd");
  }

  // Constructor called from 'Exec' query instead of the direct view links
  Inspect(Key k) {
    _key.reset();
    _key.check(this, k.toString());
    _offset.reset();
    _offset.check(this, "");
    _max_column.reset();
    _max_column.check(this, "");
    _view.reset();
    _view.check(this, "");
  }

  // Default no-args constructor
  Inspect() {
  }

  public static Response redirect(JsonObject resp, Job keyProducer, Key dest) {
    JsonObject redir = new JsonObject();
    if (keyProducer!=null) redir.addProperty(JOB, keyProducer.self().toString());
    redir.addProperty(KEY, dest.toString());
    return Response.redirect(resp, Inspect.class, redir);
  }

  public static Response redirect(JsonObject resp, Key dest) {
    return redirect(resp, null, dest);
  }

  public static Response redirect(Request req, Key dest) {
    return Response.redirect(req, "Inspect", KEY, dest );
  }

  public static String link(String txt, Key key) {
    return "<a href='Inspect.html?key=" + key + "'>" + txt + "</a>";
  }

  @Override protected boolean log() {
    return false;
  }

  @Override
  protected Response serve() {
    // Key might not be the same as Value._key, e.g. a user key
    Key key = Key.make(_key.record()._originalValue);
    Value val = _key.value();
    if(val == null) {
      // Some requests redirect before creating dest
      return RequestServer._http404.serve();
    }
    if( val.type() == TypeMap.PRIM_B )
      return serveUnparsedValue(key, val);
    Freezable f = val.getFreezable();
    if( f instanceof ValueArray ) {
      ValueArray ary = (ValueArray)f;
      if( ary._cols.length==1 && ary._cols[0]._name==null )
        return serveUnparsedValue(key, val);

      int columns_to_display = 0;
      if (_max_column.value() > 0)
        columns_to_display = _max_column.value();

      return serveValueArray(ary, columns_to_display);
    }
    if( f instanceof Vec ) {
      return serveUnparsedValue(key, ((Vec) f).chunkIdx(0));
    }
    if( f instanceof Frame ) {
      return serveFrame(key, (Frame) f);
    }
    if( f instanceof GLMModel ) {
      GLMModel m = (GLMModel)f;
      JsonObject res = new JsonObject();
      res.add(GLMModel.NAME, m.toJson());
      Response r = Response.done(res);
      r.setBuilder(ROOT_OBJECT, new GLMBuilder(m, null));
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
      RFModel rfModel = (RFModel)f;
      JsonObject response = new JsonObject();
      return RFView.redirect(response, rfModel._key, rfModel._dataKey, true);
    }
    /*if( f instanceof PCAModel ) {
      PCAModel m = (PCAModel)f;
      JsonObject res = new JsonObject();
      res.add(PCAModel.NAME, m.toJson());
      Response r = Response.done(res);
      r.setBuilder(PCAModel.NAME, new PCA.Builder(m));
      return r;
    }*/
    if( f instanceof Job.Fail ) {
      UKV.remove(val._key);   // Not sure if this is a good place to do this
      return Response.error(((Job.Fail)f)._message);
    }
    if(f instanceof hex.glm.GLMModel)
      return GLMModelView.redirect(this, key);
    if(f instanceof GBMModel)
      return GBMModelView.redirect(this, key);
//    if( f instanceof GLMValidation)
//      return GLMValidationView.redirect(this, key);
    if(f instanceof NeuralNetModel)
      return NeuralNetModelView.redirect(this, key);
    if(f instanceof DeepLearningModel)
      return DeepLearningModelView.redirect(this, key);
    if(f instanceof KMeans2Model)
      return KMeans2ModelView.redirect(this, key);
    if(f instanceof GridSearch)
      return ((GridSearch) f).redirect();
    if(f instanceof hex.pca.PCAModel)
      return PCAModelView.redirect(this, key);
    if(f instanceof hex.nb.NBModel)
      return NBModelView.redirect(this, key);
    if (f instanceof DRFModel)
      return DRFModelView.redirect(this, key);
    if (f instanceof GapStatisticModel)
      return GapStatisticModelView.redirect(this, key);
    if (f instanceof SpeeDRFModel)
      return SpeeDRFModelView.redirect(this, key);
    return Response.error("No idea how to display a "+f.getClass());
  }

  // Build a response JSON
  private final Response serveUnparsedValue(Key key, Value v) {
    JsonObject result = new JsonObject();
    result.addProperty(VALUE_TYPE, "unparsed");
    byte [] bits = v.getFirstBytes();
    bits = Utils.unzipBytes(bits, Utils.guessCompressionMethod(bits));
    PSetupGuess sguess = ParseDataset.guessSetup(bits);
    if(sguess != null && sguess._data != null && sguess._data[1].length > 0 ) { // Able to parse sanely?
      int zipped_len = v.getFirstBytes().length;
      double bytes_per_row = (double) zipped_len / sguess._data.length;
      long rows = (long) (v.length() / bytes_per_row);
      result.addProperty(NUM_ROWS, "~" + rows); // approx rows
      result.addProperty(NUM_COLS, sguess._data[1].length);

      result.add(ROWS, new Gson().toJsonTree(sguess._data));
    } else {
      result.addProperty(NUM_ROWS, "unknown");
      result.addProperty(NUM_COLS, "unknown");
    }
    result.addProperty(VALUE_SIZE, v.length());

    // The builder Response
    Response r = Response.done(result);
    // Some nice links in the response
    r.addHeader("<div class='alert'>" +
                Parse.link(key, "Parse into hex format") +
                "</div>");
    // Set the builder for showing the rows
    r.setBuilder(ROWS, new ArrayBuilder() {
      @Override public String caption(JsonArray array, String name) {
        return "<h4>First few sample rows</h4>";
      }
    });
    return r;
  }


  /**
   * serve the value array with a capped # of columns [0,max_columns)
   */
  public Response serveValueArray(final ValueArray va, int max_column) {
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

    final int col_limit = Math.min(max_column, va._cols.length);

    for( int i = 0; i < col_limit; i++ ) {
      Column c = va._cols[i];
      JsonObject json = new JsonObject();
      json.addProperty(NAME, c._name);
      json.addProperty(OFFSET, c._off);
      json.addProperty(SIZE, Math.abs(c._size));
      json.addProperty(BASE, c._base);
      json.addProperty(SCALE, (int) c._scale);
      if(c.isEnum() || Double.isNaN(c._min)) json.addProperty(MIN, "\"NaN\"");
      else json.addProperty(MIN, c._min);
      if(c.isEnum() || Double.isNaN(c._max)) json.addProperty(MAX, "\"NaN\"");
      else json.addProperty(MAX, c._max);
      if(c.isEnum() || Double.isNaN(c._mean)) json.addProperty(MEAN, "\"NaN\"");
      else json.addProperty(MEAN, c._mean);
      if(c.isEnum() || Double.isNaN(c._sigma)) json.addProperty(VARIANCE, "\"NaN\"");
      else json.addProperty(VARIANCE, c._sigma);
      json.addProperty(NUM_MISSING_VALUES, va._numrows - c._n);
      json.addProperty(TYPE, c.isEnum() ? "enum" : (c.isFloat() ? "float" : "int"));
      json.addProperty(ENUM_DOMAIN_SIZE, c.isEnum() ? c._domain.length : 0);
      cols.add(json);
    }

    if( _offset.value() != INFO_PAGE ) {
      long endRow = Math.min(_offset.value() + _view.value(), va._numrows);
      long startRow = Math.min(_offset.value(), va._numrows - _view.value());
      for( long row = Math.max(0, startRow); row < endRow; ++row ) {
        JsonObject obj = new JsonObject();
        obj.addProperty(ROW, row);
        for( int i = 0; i < col_limit; ++i )
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
        String s = html(va._key, va._numrows, va._cols.length, va._rowsize, va.length());
        Table t = new Table(argumentsToJson(), _offset.value(), _view.value(), va, col_limit);
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

  private static void format(JsonObject obj, Frame f, long rowIdx, int colIdx) {
    Vec v = f.vecs()[colIdx];
    if( rowIdx < 0 || rowIdx >= v.length() )
      return;
    String name = f._names[colIdx] != null ? f._names[colIdx] : "" + colIdx;
    if( v.isNA(rowIdx) ) obj.addProperty(name, "NA");
    else if( v.isEnum() ) obj.addProperty(name, v.domain((int)v.at8(rowIdx)));
    else if( v.isInt() ) obj.addProperty(name, v.at8(rowIdx));
    else obj.addProperty(name, v.at(rowIdx));
  }

  private final String html(Key key, long rows, int cols, int bytesPerRow, long bytes) {
    String keyParam = KEY + "=" + key.toString();
    StringBuilder sb = new StringBuilder();
    // @formatter:off
    sb.append(""
        + "<h3>"
          + "<a href='RemoveAck.html?" + keyParam + "'>"
          + "<button class='btn btn-danger btn-mini'>X</button></a>"
          + "&nbsp;&nbsp;" + key.toString()
        + "</h3>");
    if (_producer.valid() && _producer.value()!=null) {
      Job job = Job.findJob(Key.make(_producer.value()));
      if (job!= null)
        sb.append("<div class='alert alert-success'>"
        		+ "<b>Produced in ").append(PrettyPrint.msecs(job.runTimeMs(),true)).append(".</b></div>");
    }
    sb.append("<div class='alert alert-info'>").append(Inspect4UX.link(key, "NEW Inspect!")).append("</div>");
    sb.append("<div class='alert'>Set " + SetColumnNames.link(key,"Column Names") +"<br/>View " + SummaryPage.link(key, "Summary") +  "<br/>Build models using "
          + PCA.link(key, "PCA") + ", "
          + RF.link(key, "Single Node Random Forest") + ", "
          + DRF.link(key, "Distributed Random Forest") + ", "
          + GBM.link(key, "GBM") + ", "
          + GLM.link(key, "GLM") + ", " + GLMGrid.link(key, "GLM Grid Search") + ", "
          + GLM2.link(key, "Generalized Linear Modeling") +", "
          + KMeans.link(key, "KMeans") + ", "
//          + NeuralNet.link(key, NeuralNet.DOC_GET) + ", or "
          + DeepLearning.link(key, DeepLearning.DOC_GET) + "<br />"
          + "Score data using "
          + RFScore.link(key, "Random Forest") + ", "
          + GLMScore.link(KEY, key, "0.0", "GLM") + "</br><b>Download as</b> " + DownloadDataset.link(key, "CSV")
        + "</div>"
        + "<p><b><font size=+1>"
          + cols + " columns"
          + (bytesPerRow != 0 ? (", " + bytesPerRow + " bytes-per-row * " + String.format("%,d",rows) + " rows = " + PrettyPrint.bytes(bytes)) : "")
        + "</font></b></p>");
      // sb.append(
      // " <script>$('#inspect').submit( function() {" +
      // " $('html', 'table').animate({ scrollTop: $('#row_30').offset().top" +
      // "}, 2000);" +
      // "return false;" +
      // "});</script>");
      String _scrollto = String.valueOf(_offset.value() - 1);
      sb.append(
      " <script>$(document).ready(function(){ " +
      " $('html, body').animate({ scrollTop: $('#row_"+_scrollto+"').offset().top" +
      "}, 2000);" +
      "return false;" +
      "});</script>");
    sb.append(
        "<form class='well form-inline' action='Inspect.html' id='inspect'>" +
        " <input type='hidden' name='key' value="+key.toString()+">" +
        " <input type='text' class='input-small span5' placeholder='filter' " +
        "    name='offset' id='offset' value='"+_offset.value()+"' maxlength='512'>" +
        " <button type='submit' class='btn btn-primary'>Jump to row!</button>" +
        "</form>");
    // @formatter:on
    return sb.toString();
  }

  // Frame

  public Response serveFrame(final Key key, final Frame f) {
    if( _offset.value() > f.numRows() )
      return Response.error("Value only has " + f.numRows() + " rows");

    JsonObject result = new JsonObject();
    result.addProperty(VALUE_TYPE, "parsed");
    result.addProperty(KEY, key.toString());
    result.addProperty(NUM_ROWS, f.numRows());
    result.addProperty(NUM_COLS, f.numCols());

    JsonArray cols = new JsonArray();
    JsonArray rows = new JsonArray();

    for( int i = 0; i < f.numCols(); i++ ) {
      Vec v = f.vecs()[i];
      JsonObject json = new JsonObject();
      json.addProperty(NAME, f._names[i]);
      json.addProperty(TYPE, v.isEnum() ? ColType.Enum.toString() : v.isInt() ? ColType.Int.toString() : ColType.Real.toString());
      json.addProperty(MIN, v.isEnum() ? Double.NaN : v.min());
      json.addProperty(MAX, v.isEnum() ? Double.NaN : v.max());
      json.addProperty(CARDINALITY, v.cardinality());
      cols.add(json);
    }

    if( _offset.value() != INFO_PAGE ) {
      long endRow = Math.min(_offset.value() + _view.value(), f.numRows());
      long startRow = Math.min(_offset.value(), f.numRows() - _view.value());
      for( long row = Math.max(0, startRow); row < endRow; ++row ) {
        JsonObject obj = new JsonObject();
        obj.addProperty(ROW, row);
        for( int i = 0; i < f.numCols(); ++i )
          format(obj, f, row, i);
        rows.add(obj);
      }
    }

    result.add(COLS, cols);
    result.add(ROWS, rows);

    Response r = Response.done(result);
    r.setBuilder(ROOT_OBJECT, new ObjectBuilder() {
      @Override
      public String build(Response response, JsonObject object, String contextName) {
        String s = html(key, f.numRows(), f.numCols(), 0, 0);
        Table2 t = new Table2(argumentsToJson(), _offset.value(), _view.value(), f);
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

  private static final class Table extends PaginatedTable {
    private final ValueArray _va;
    private final int _max_columns;

    public Table(JsonObject query, long offset, int view, ValueArray va, int max_columns_to_display) {
      super(query, offset, view, va._numrows, true);
      _va = va;
      _max_columns = Math.min(max_columns_to_display, va._cols.length);
    }

    public Table(JsonObject query, long offset, int view, ValueArray va) {
      super(query, offset, view, va._numrows, true);
      _va = va;
      _max_columns = va._cols.length;
    }

    @Override
    public String build(Response response, JsonArray array, String contextName) {
      StringBuilder sb = new StringBuilder();

      if (_va._cols.length > _max_columns)
        sb.append("<p style='text-align:center;'><center><h5 style='font-weight:800; color:red;'>Columns trimmed to " + _max_columns + "</h5></center></p>");
      if( array.size() == 0 ) { // Fake row, needed by builder
        array = new JsonArray();
        JsonObject fake = new JsonObject();
        fake.addProperty(ROW, 0);
        for( int i = 0; i < _max_columns; ++i )
          format(fake, _va, 0, i);
        array.add(fake);
      }
      sb.append(header(array));

      JsonObject row = new JsonObject();

      row.addProperty(ROW, "Change Type");
      String k = _va._key.toString();
      for( int i = 0; i < _max_columns; i++ ) {
        if(_va._cols[i].isFloat()) {
          row.addProperty(_va._cols[i]._name, "");
          continue;
        }
        if(_va._cols[i].isEnum()) {
          // check if previously an int column
          // technically I should check if the whole domain is ints, I just check mu and sigma instead.
          // enums have mu = sigma = NaN, unless they are just transformed int column.
          if(!Double.isNaN(_va._cols[i]._mean) && !Double.isNaN(_va._cols[i]._sigma)){
            String btn = "<span class='btn_custom'>\n";
            btn += "<a href='ToEnum.html?key=" + k + "&col_index=" + (i)  + "&to_enum=false" + "'>\n"
              + "<button type='submit' class='btn btn-custom'>As Integer</button>\n";
            btn += "</span>\n";
            row.addProperty(_va._cols[i]._name, btn);
          } else row.addProperty(_va._cols[i]._name, "");
          continue;
        }
        String btn = "<span class='btn_custom'>\n";
        btn += "<a href='ToEnum.html?key=" + k + "&col_index=" + (i)  + "&to_enum=true" + "'>"
                + "<button type='submit' class='btn btn-custom'>As Factor</button>";
        btn += "</span>\n";
        row.addProperty(_va._cols[i]._name, btn);
      }
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      row.addProperty(ROW, TYPE);
      for( int i = 0; i < _max_columns; i++ )
        row.addProperty(_va._cols[i]._name, _va._cols[i].isEnum() ? ColType.Enum.toString() : _va._cols[i].isFloat() ? ColType.Real.toString() : ColType.Int.toString());
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      row.addProperty(ROW, MIN);
      for( int i = 0; i < _max_columns; i++ )
        row.addProperty(_va._cols[i]._name, _va._cols[i].isEnum() ? Double.NaN : _va._cols[i]._min);
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      row.addProperty(ROW, MAX);
      for( int i = 0; i < _max_columns; i++ )
        row.addProperty(_va._cols[i]._name, _va._cols[i].isEnum() ? Double.NaN : _va._cols[i]._max);
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      row.addProperty(ROW, MEAN);
      for( int i = 0; i < _max_columns; i++ )
        row.addProperty(_va._cols[i]._name, _va._cols[i].isEnum() ? Double.NaN : _va._cols[i]._mean);
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      row.addProperty(ROW, VARIANCE);
      for( int i = 0; i < _max_columns; i++ )
        row.addProperty(_va._cols[i]._name, _va._cols[i].isEnum() ? Double.NaN : _va._cols[i]._sigma);
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      row.addProperty(ROW, CARDINALITY);
      for( int i = 0; i < _max_columns; i++ )
        row.addProperty(_va._cols[i]._name, _va._cols[i].isEnum() ? (long) (_va._cols[i]._max-_va._cols[i]._min+1) : Double.NaN);
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      row.addProperty(ROW, NUM_MISSING_VALUES);
      for( int i = 0; i < _max_columns; i++ )
        row.addProperty(_va._cols[i]._name, _va._numrows - _va._cols[i]._n);
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      if( _offset == INFO_PAGE ) {
        row.addProperty(ROW, OFFSET);
        for( int i = 0; i < Math.min(MAX_COLUMNS_TO_DISPLAY,_va._cols.length); i++ )
          row.addProperty(_va._cols[i]._name, _va._cols[i]._off);
        sb.append(defaultBuilder(row).build(response, row, contextName));

        row.addProperty(ROW, SIZE);
        for( int i = 0; i < _max_columns; i++ )
          row.addProperty(_va._cols[i]._name, Math.abs(_va._cols[i]._size));
        sb.append(defaultBuilder(row).build(response, row, contextName));

        row.addProperty(ROW, BASE);
        for( int i = 0; i < _max_columns; i++ )
          row.addProperty(_va._cols[i]._name, _va._cols[i]._base);
        sb.append(defaultBuilder(row).build(response, row, contextName));

        row.addProperty(ROW, SCALE);
        for( int i = 0; i < _max_columns; i++ )
          row.addProperty(_va._cols[i]._name, (int) _va._cols[i]._scale);
        sb.append(defaultBuilder(row).build(response, row, contextName));

        row.addProperty(ROW, ENUM_DOMAIN_SIZE);
        for( int i = 0; i < _max_columns; i++ )
          row.addProperty(_va._cols[i]._name, _va._cols[i].isEnum() ? _va._cols[i]._domain.length : 0);
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
      if (_va._cols.length > _max_columns)
        sb.append("<p style='text-align:center;'><center><h5 style='font-weight:800; color:red;'>Columns trimmed to " + _max_columns + "</h5></center></p>");
      return sb.toString();
    }
  }

  private static final class Table2 extends PaginatedTable {
    private final Frame _f;

    public Table2(JsonObject query, long offset, int view, Frame f) {
      super(query, offset, view, f.numRows(), true);
      _f = f;
    }

    @Override
    public String build(Response response, JsonArray array, String contextName) {
      StringBuilder sb = new StringBuilder();
      if( array.size() == 0 ) { // Fake row, needed by builder
        array = new JsonArray();
        JsonObject fake = new JsonObject();
        fake.addProperty(ROW, 0);
        for( int i = 0; i < _f.numCols(); ++i )
          format(fake, _f, 0, i);
        array.add(fake);
      }
      sb.append(header(array));

      JsonObject row = new JsonObject();

      row.addProperty(ROW, TYPE);
      for( int i = 0; i < _f.numCols(); i++ )
        row.addProperty(_f._names[i], _f.vecs()[i].isEnum() ? ColType.Enum.toString() : _f.vecs()[i].isInt() ? ColType.Int.toString() : ColType.Real.toString());
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      row.addProperty(ROW, MIN);
      for( int i = 0; i < _f.numCols(); i++ )
        row.addProperty(_f._names[i], _f.vecs()[i].isEnum() ? Double.NaN : _f.vecs()[i].min());
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      row.addProperty(ROW, MAX);
      for( int i = 0; i < _f.numCols(); i++ )
        row.addProperty(_f._names[i], _f.vecs()[i].isEnum() ? Double.NaN : _f.vecs()[i].max());
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      row.addProperty(ROW, CARDINALITY);
      for( int i = 0; i < _f.numCols(); i++ )
        row.addProperty(_f._names[i], _f.vecs()[i].isEnum() ? _f.vecs()[i].cardinality() : Double.NaN);
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      row.addProperty(ROW, FIRST_CHUNK);
      for( int i = 0; i < _f.numCols(); i++ )
        row.addProperty(_f._names[i], _f.vecs()[i].chunkForChunkIdx(0).getClass().getSimpleName());
      sb.append(ARRAY_HEADER_ROW_BUILDER.build(response, row, contextName));

      if( _offset == INFO_PAGE ) {

        for( int ci = 0; ci < _f.vecs()[0].nChunks(); ci++ ) {
//          Chunk chunk = _f.vecs()[ci].chunkForChunkIdx(ci);
          String prefix = CHUNK + " " + ci + " ";
          row.addProperty(ROW, prefix + TYPE);
          for( int i = 0; i < _f.numCols(); i++ )
            row.addProperty(_f._names[i], _f.vecs()[i].chunkForChunkIdx(ci).getClass().getSimpleName());
          sb.append(defaultBuilder(row).build(response, row, contextName));
          row.addProperty(ROW, prefix + SIZE);
          for( int i = 0; i < _f.numCols(); i++ )
            row.addProperty(_f._names[i], _f.vecs()[i].chunkForChunkIdx(ci).byteSize());
          sb.append(defaultBuilder(row).build(response, row, contextName));
        }
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

  @Override
  public RequestServer.API_VERSION[] supportedVersions() {
    return SUPPORTS_ONLY_V1;
  }
}
