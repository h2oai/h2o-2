package water.api;

import java.util.*;

import hex.*;
import hex.DPCA.*;
import hex.NewRowVecTask.DataFrame;
import water.*;
import water.api.RequestArguments.Argument;
import water.util.Log;
import water.util.RString;

import com.google.common.base.Objects;
import com.google.common.primitives.Ints;
import com.google.gson.*;

public class PCA extends Request {
  protected final H2OKey _dest = new H2OKey(DEST_KEY, PCAModel.makeKey());
  protected final H2OHexKey _key = new H2OHexKey(KEY);
  protected final HexColumnSelect _ignore = new PCAColumnSelect(IGNORE, _key);
  // protected final Int _numPC = new Int("num_pc", 10, 1, 1000000);
  protected final Real _tol = new Real("tolerance", 0.0, 0, 1, "Omit components with std dev <= tol times std dev of first component");
  protected final Bool _standardize = new Bool("standardize", true, "Set to standardize (0 mean, unit variance) the data before training.");

  public PCA() {
    _requestHelp = "Compute principal components of a data set.";
    _ignore._requestHelp = "A list of ignored columns (specified by name or 0-based index).";
    // _numPC._requestHelp = "Number of principal components to return.";
    _tol._requestHelp = "Components omitted if their standard deviations are <= tol times standard deviation of first component.";
  }


  PCAParams getPCAParams() {
    // PCAParams res = new PCAParams(_numPC.value());
    PCAParams res = new PCAParams(_tol.value());
    return res;
  }

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='PCA.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public static String link(Key k, double tol, String content) {
    StringBuilder sb = new StringBuilder("<a href='PCA.query?");
    sb.append(KEY + "=" + k.toString());
    sb.append("&tolerance=" + tol);
    sb.append("'>" + content + "</a>");
    return sb.toString();
  }

  private int[] createColumns(ValueArray ary) {
    BitSet bs = new BitSet();
    bs.set(0, ary._cols.length);
    for( int i : _ignore.value() ) bs.clear(i);
    int cols[] = new int[bs.cardinality()];
    int idx = 0;
    for( int i=bs.nextSetBit(0); i >= 0; i=bs.nextSetBit(i+1))
      cols[idx++] = i;
    assert idx==cols.length;
    return cols;
  }

  @Override protected void queryArgumentValueSet(Argument arg, Properties inputArgs) {
    if(arg == _ignore) {
      int[] ii = _ignore.value();
      if(ii != null && ii.length >= _key.value()._cols.length)
        throw new IllegalArgumentException("Cannot ignore all columns");

      int numIgnore = ii == null ? 0 : ii.length;
      if(_key.value() != null && _key.value()._cols.length - numIgnore > _key.value()._numrows-1)
        // TODO: Degrees of freedom = num_rows - 1 if standardized, num_rows otherwise
        throw new IllegalArgumentException("Cannot have more columns than degrees of freedom = " + String.valueOf(_key.value()._numrows-1));
    }
  }

  @Override protected Response serve() {
    try {
      JsonObject j = new JsonObject();
      Key dest = _dest.value();
      ValueArray ary = _key.value();

      PCAParams pcaParams = getPCAParams();
      // int[] cols = new int[ary._cols.length];
      // for( int i = 0; i < cols.length; i++ ) cols[i] = i;

      // DataFrame data = DataFrame.makePCAData(ary, cols, true);
      DataFrame data = DataFrame.makePCAData(ary, createColumns(ary), _standardize.value());
      PCAJob job = DPCA.startPCAJob(dest, data, pcaParams);
      j.addProperty(JOB, job.self().toString());
      j.addProperty(DEST_KEY, job.dest().toString());

      Response r = Progress.redirect(j, job.self(), job.dest());
      r.setBuilder(Constants.DEST_KEY, new KeyElementBuilder());
      return r;
    } catch(RuntimeException e) {
      Log.err(e);
      return Response.error(e.getMessage());
    } catch (Throwable t) {
      Log.err(t);
      return Response.error(t.getMessage());
    }
  }

  static class Builder extends ObjectBuilder {
    final PCAModel _m;

    Builder(PCAModel m) {
      _m = m;
    }

    public String build(Response response, JsonObject json, String contextName) {
      StringBuilder sb = new StringBuilder();
      modelHTML(_m, json, sb);
      return sb.toString();
    }

    private void modelHTML(PCAModel m, JsonObject json, StringBuilder sb) {
      sb.append("<span style='display: inline-block;'>");
      sb.append("<table class='table table-striped table-bordered'>");
      sb.append("<tr>");
      sb.append("<th>Feature</th>");
      for(int i = 0; i < m._num_pc; i++)
        sb.append("<th>").append("PC" + i).append("</th>");
      sb.append("</tr>");

      // Row of standard deviation values
      sb.append("<tr class='warning'>");
      // sb.append("<td>").append("&sigma;").append("</td>");
      sb.append("<td>").append("Std Dev").append("</td>");
      for(int c = 0; c < m._sdev.length; c++)
        sb.append("<td>").append(ElementBuilder.format(m._sdev[c])).append("</td>");
      sb.append("</tr>");

      // Row with proportion of variance
      sb.append("<tr class='warning'>");
      // sb.append("<td>").append("Prop &sigma;<sup>2</sup>").append("</td>");
      sb.append("<td>").append("Prop Var").append("</td>");
      for(int c = 0; c < m._propVar.length; c++)
        sb.append("<td>").append(ElementBuilder.format(m._propVar[c])).append("</td>");
      sb.append("</tr>");

      // Each row is component of eigenvector
      for( int r = 0; r < m._va._cols.length; r++ ) {
        sb.append("<tr>");
        sb.append("<th>").append(m._va._cols[r]._name).append("</th>");
        for( int c = 0; c < m._num_pc; c++ ) {
          double e = m._eigVec[c][r];
          sb.append("<td>").append(ElementBuilder.format(e)).append("</td>");
        }
        sb.append("</tr>");
      }
      sb.append("</table></span>");
    }
  }

  // By default, ignore all non-numeric columns
  class PCAColumnSelect extends HexColumnSelect {
     // double _maxNAsRatio = 0.1;
     transient ThreadLocal<TreeSet<String>> _nonNumColumns = new ThreadLocal<TreeSet<String>>();

     public PCAColumnSelect(String name, H2OHexKey key) {
       super(name, key);
     }

     @Override protected int[] defaultValue() {
       ValueArray va = _key.value();
       int [] res = new int[va._cols.length];
       int selected = 0;
       for(int i = 0; i < va._cols.length; ++i) {
         if(va._cols[i]._domain != null) {
           res[selected++] = i;
           if(_nonNumColumns.get() == null)
             _nonNumColumns.set(new TreeSet<String>());
           _nonNumColumns.get().add(Objects.firstNonNull(va._cols[i]._name, String.valueOf(i)));
         }
       }
       return Arrays.copyOfRange(res,0,selected);
     }

     @Override protected String queryDescription() {
       return "Columns to ignore";
     }

     public String queryComment() {
       if(_nonNumColumns.get() == null || _nonNumColumns.get().isEmpty()) return "";
       TreeSet<String> ignoredCols = _nonNumColumns.get();
       return "<div class='alert'><b>Ignoring " + _nonNumColumns.get().size() + " non-numeric columns</b>: " + ignoredCols.toString() +"</div>";
     }
  }
}