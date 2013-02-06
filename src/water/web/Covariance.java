package water.web;
import java.util.Properties;

import water.PrettyPrint;
import water.ValueArray;

import com.google.gson.JsonObject;

public class Covariance extends H2OPage {
  @Override
  public JsonObject serverJson(Server server, Properties args, String sessionID) throws PageError {
    ValueArray ary = ServletUtil.check_array(args,"Key");
    int colA = parseColumnNameOrIndex(ary, args.getProperty("colA", "0"));
    int colB = parseColumnNameOrIndex(ary, args.getProperty("colB", "1"));
    return hex.Covariance.run(ary,colA,colB);
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    JsonObject json = serverJson(server, args, sessionID);
    RString html = new RString(
        "Covariance of <a href='/Inspect?Key=%$Key'>%Key</a> between columns %ColA and %ColB" +
        "<p>Pass 1 took %p1, pass 2 took %p2" +
        "<p>Covariance  = %Covariance" +
        "<p>Correlation = %Correlation" +
        "<p><table><tr><td></td><td>Var %ColA</td><td>Var %ColB</td></tr>" +
        "<tr><td>Mean</td><td>%XMean</td><td>%YMean</td></tr>" +
        "<tr><td>Standard Deviation</td><td>%XStdDev</td><td>%YStdDev</td></tr>" +
        "<tr><td>Variance</td><td>%XVariance</td><td>%YVariance</td></tr>" +
        "</table>" );
    html.replace(json);
    html.replace("p1", PrettyPrint.msecs(json.get("Pass1Msecs").getAsLong(), true));
    html.replace("p2", PrettyPrint.msecs(json.get("Pass2Msecs").getAsLong(), true));
    return html.toString();
  }
}
