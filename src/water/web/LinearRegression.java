package water.web;
import java.util.Properties;

import com.google.gson.JsonObject;

import water.PrettyPrint;
import water.ValueArray;

public class LinearRegression extends H2OPage {

  @Override
  public JsonObject serverJson(Server server, Properties args, String sessionID) throws PageError {
    ValueArray ary = ServletUtil.check_array(args,"Key");
    int colA = parseColumnNameOrIndex(ary, args.getProperty("colA", "0"));
    int colB = parseColumnNameOrIndex(ary, args.getProperty("colB", "1"));
    return hex.LinearRegression.run(ary,colA,colB);
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    JsonObject json = serverJson(server, args, sessionID);

    RString html = new RString(
        "Linear Regression of <a href='/Inspect?Key=%$Key'>%Key</a> between columns %ColA and %ColB" +
        "<p>Pass 1 took %p1, pass 2 took %p2, Pass 3 took %p3" +
        "<p>Found <b>%Rows</b>" +
        "<p><b>y = %Beta1 * x + %Beta0</b>" +
        "<p>R^2 = %RSquared" +
        "<p>std error of beta_1 = %Beta1StdErr" +
        "<p>std error of beta_0 = %Beta0StdErr" +
        "<p>SSTO = %SSTO" +
        "<p>SSE = %SSE" +
        "<p>SSR = %SSR" );
    html.replace(json);
    html.replace("p1", PrettyPrint.msecs(json.get("Pass1Msecs").getAsLong(), true));
    html.replace("p2", PrettyPrint.msecs(json.get("Pass2Msecs").getAsLong(), true));
    html.replace("p3", PrettyPrint.msecs(json.get("Pass3Msecs").getAsLong(), true));
    return html.toString();
  }
}
