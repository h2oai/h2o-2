package water.web;
import com.google.gson.JsonObject;
import java.util.Properties;
import water.Key;
import water.exec.PositionedException;

/**
 * The servlet for launching a computation
 *
 * @author cliffc@0xdata.com
 */
public class ExecWeb extends H2OPage {
  @Override
  public JsonObject serverJson(Server server, Properties args, String sessionID) throws PageError {
    // Get parameters: Key, file name, replication factor
    String x = args.getProperty("Expr");
    if( x==null || x.isEmpty() ) throw new PageError("Expression is missing");
    JsonObject res = new JsonObject();
    try {
      long time = System.currentTimeMillis();
      Key k = water.exec.Exec.exec(x);
      time = System.currentTimeMillis() - time;
      res.addProperty("Expr", x);
      res.addProperty("ResultKey", k.toString());
      res.addProperty("Time",time);
    } catch( PositionedException e ) {
      res.addProperty("Expr", x);
      res.addProperty("Error", e.report(x));
    }
    return res;
  }

  //
  @Override protected String serveImpl(Server server, Properties args, String sessionId) throws PageError {
    RString query = new RString(ExecQuery.html);
    query.replace("expr",args.getProperty("Expr"));
    try {
      long time = System.currentTimeMillis();
      Key k = water.exec.Exec.exec(args.getProperty("Expr"));
      time = System.currentTimeMillis() - time;
      args.put("Key",k.toString());
      return query.toString() + "<p>Query execution took "+(time/1000.0)+" [s]</p>"+ new Inspect().serveImpl(server,args,sessionId);
    } catch (PositionedException e) {
      return query.toString() + error("<span style='font-family:monospace'>"+e.reportHTML(args.getProperty("Expr")) +"</span>");
    }
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Expr" };
  }
}
