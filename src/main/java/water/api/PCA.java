package water.api;

import hex.*;
import hex.DPCA.*;
import hex.NewRowVecTask.DataFrame;

import water.*;
import water.api.RequestBuilders.*;
import water.util.Log;
import water.util.RString;

import com.google.gson.*;

public class PCA extends Request {
  protected final H2OKey _dest = new H2OKey(DEST_KEY, PCAModel.makeKey());
  protected final H2OHexKey _key = new H2OHexKey(KEY);
  protected final Int _num_pc = new Int("num_pc", 10, 1, 10000);

  public PCA() {
    _requestHelp = "Compute principal components of a data set.";
    _num_pc._requestHelp = "Number of principal components.";
  }

  PCAParams getPCAParams() {
    PCAParams res = new PCAParams(_num_pc.value());
    return res;
  }

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='PCA.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public static String link(Key k, int num_pc, String content) {
    StringBuilder sb = new StringBuilder("<a href='PCA.query?");
    sb.append(KEY + "=" + k.toString());
    sb.append("&num_pc=" + num_pc);
    sb.append("'>" + content + "</a>");
    return sb.toString();
  }

  @Override protected Response serve() {
    try {
      JsonObject j = new JsonObject();
      Key dest = _dest.value();
      ValueArray ary = _key.value();

      PCAParams pcaParams = getPCAParams();
      int[] cols = new int[ary._cols.length];
      for( int i = 0; i < cols.length; i++ ) cols[i] = i;
      DataFrame data = DGLM.getData(ary, cols, null, true);

      PCAJob job = DPCA.startPCAJob(dest, data, pcaParams);
      j.addProperty(JOB, job.self().toString());
      j.addProperty(DEST_KEY, job.dest().toString());

      Response r = Progress.redirect(j, job.self(), job.dest());
      r.setBuilder(Constants.DEST_KEY, new KeyElementBuilder());
      return r;

      /*
      JsonObject resPCA = DPCA.buildModel(null, dest, data, pcaParams._num_pc).toJson();
      Response r = Response.done(resPCA);
      return r;
      */
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
      for( int i = 0; i < m._pcaParams._num_pc; i++)
        sb.append("<th>").append("PC" + i).append("</th>");
      sb.append("</tr>");

      // Row of standard deviation values
      sb.append("<tr class='warning'>");
      sb.append("<td>").append("&sigma;").append("</td>");
      for(int c = 0; c < m._sdev.length; c++)
        sb.append("<td>").append(ElementBuilder.format(m._sdev[c])).append("</td>");
      sb.append("</tr>");

      // Each row is component of eigenvector
      for( int r = 0; r < m._va._cols.length; r++ ) {
        sb.append("<tr>");
        sb.append("<th>").append(m._va._cols[r]._name).append("</th>");
        for( int c = 0; c < m._pcaParams._num_pc; c++ ) {
          double e = m._eigVec[c][r];
          sb.append("<td>").append(ElementBuilder.format(e)).append("</td>");
        }
        sb.append("</tr>");
      }
      sb.append("</table></span>");
    }
  }
}