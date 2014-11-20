package water.api;

import hex.la.DMatrix.MatrixMulStats;
import water.*;
import water.fvec.Frame;

/**
 * Created by tomasnykodym on 11/17/14.
 */
public class MMStats  extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Inspect a fluid-vec frame";
  static final String NA = ""; // not available information

  @API(help = "An existing H2O key pointin to MatrixMulStats object.", required = true, filter = Default.class, gridable = false)
  Key src_key;


  MatrixMulStats _stats;

  @Override
  protected Response serve() {
    Value v = DKV.get(src_key);
    if (v == null)
      return Response.error("key(\"" + src_key + "\" does not exist!");
    try {
      _stats = v.get();
    } catch(Exception e) { // if job is done
      return Inspector.redirect(this,src_key);
    }
    float progress = (float) _stats.chunksDone / _stats.chunksTotal;
    return Response.poll(this, (int) (100 * progress), 100, "job_key", _stats.jobKey, "destination_key", src_key);
  }

  public String prettyprint(long l){
    long gigs = l >> 30;
    long megs = (l-gigs) >> 20;
    long kbs = (l - (gigs << 30) - (megs << 20)) >> 10;
    long bytes = l - (gigs << 30) - (megs << 20) - (kbs << 10);
    return (gigs > 0?(gigs + "GB "):"") + (megs > 0?(megs + "MB "):("")) + (kbs > 0?(kbs + "KB "):("")) + bytes + "B";
  }
  public String pprintTime(long l){
    long secs = l/1000;
    long hrs = secs/3600;
    long min = (secs-hrs*3600)/60;
    secs = secs - hrs*3600 - min*60;
    return hrs + "hrs " + min + "min " + secs + "s";
  }
  @Override
  public boolean toHTML(StringBuilder sb) {
    if(_stats == null)return true;
    DocGen.HTML.arrayHead(sb);
    // Column labels

//    " <button type='submit' class='btn btn-primary'>Jump to row!</button>" +
    sb.append("<tr>");
    sb.append("<td>").append("run time").append("</td><td>" + pprintTime(_stats.lastUpdateAt - _stats._startTime) + "</td>");
    sb.append("</tr><tr>");
    sb.append("<td>").append("chunks total").append("</td><td>" + _stats.chunksTotal + "</td>");
    sb.append("</tr><tr>");
    sb.append("<td>").append("chunks done").append("</td><td>" + _stats.chunksDone + "</td>");
    sb.append("</tr><tr>");
    sb.append("<td>").append("output size").append("</td><td>" + prettyprint(_stats.size) + "</td>");
    for(int i = 0; i < _stats.chunkTypes.length; ++i) {
      int ct = _stats.chunkTypes[i];
      sb.append("</tr><tr>");
      sb.append("<td>").append(TypeMap.newInstance(ct).getClass().getSimpleName()).append("</td><td>" + _stats.chunkCnts[i] +  "</td>");
    }
    sb.append("</tr>");
    DocGen.HTML.arrayTail(sb);
    return true;
  }
}
