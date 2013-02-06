package water.web;

import java.util.Properties;

import water.ValueArray;

/** H2O branded web page.
 *
 * We might in theory not need this and it can be all pushed to Page, but this
 * is just for the sake of generality.
 *
 * @author peta
 */
public abstract class H2OPage extends Page {

  protected int _refresh = 0;

  protected abstract String serveImpl(Server server, Properties args, String sessionID) throws PageError;

  protected static final String[] EMPTY = {};
  protected String[] additionalScripts() { return EMPTY; }
  protected String[] additionalStyles()  { return EMPTY; }

  private static final String navbar =
      "              <li><a href=\"/\">Cloud</a></li>"
    + "              <li><a href=\"/StoreView\">Node</a></li>"
    + "              <li><a href=\"/GetQuery\">Get</a></li>"
    + "              <li><a href=\"/Put\">Put</a></li>"
    + "              <li><a href=\"/Timeline\">Timeline</a></li>"
    + "              <li><a href=\"/ImportQuery\">Import</a></li>"
    + "              <li><a href=\"/RFBuildQuery\">RF</a></li>"
    + "              <li><a href=\"/ExecQuery\">R</a></li>"
    + "              <li><a href=\"/DebugView\">Debug View</a></li>"
    + "              <li><a href=\"/ProgressView\">Progress View</a></li>"
    + "              <li><a href=\"/Network\">Network</a></li>"
    + "              <li><a href=\"/Shutdown\">Shutdown All</a></li>"
    ;

  @Override public String serve(Server server, Properties args, String sessionID) {
    String username = sessionID != null ? Server._sessionManager.authenticate(sessionID) : null;
    RString response = new RString(html);
    response.replace("navbar",navbar);
    if (username != null)
      response.replace("footer","You are logged as "+username+". <a href=\"logoff\">Logoff</a>");
    try {
      String result = serveImpl(server, args, sessionID);
      if (result == null) return result;
      if (_refresh!=0) response.replace("refresh","<META HTTP-EQUIV='refresh' CONTENT='"+_refresh+"'>");

      // Append additional scripts
      StringBuilder additions = new StringBuilder();
      for(String script : additionalScripts()) {
        additions.append("<script src='");additions.append(script);additions.append("'></script>");
      }
      response.replace("scripts", additions.toString());
      // Append additional styles <link href=\"bootstrap/css/bootstrap.css\" rel=\"stylesheet\">"
      additions.setLength(0);
      for(String style : additionalStyles()) {
        additions.append("<link href='");additions.append(style);additions.append("' rel='stylesheet'>");
      }
      response.replace("styles", additions.toString());

      response.replace("contents",result);
    } catch (PageError e) {
      response.replace("contents", e.getMessage());
    }
    return response.toString();
  }


  private static final String html_notice =
            "<div class='alert %atype'>"
          + "%notice"
          + "</div>"
          ;

  public static String error(String text) {
    RString notice = new RString(html_notice);
    notice.replace("atype","alert-error");
    notice.replace("notice",text);
    return notice.toString();
  }

  public static String success(String text) {
    RString notice = new RString(html_notice);
    notice.replace("atype","alert-success");
    notice.replace("notice",text);
    return notice.toString();
  }

  public static String wrap(String what) {
    RString response = new RString(html);
    response.replace("contents",what);
    return response.toString();
  }

  public static int getAsNumber(Properties args, String arg, int def) {
    int result = def;
    try {
      String s = args.getProperty(arg,"");
      if (!s.isEmpty())
        result = Integer.parseInt(s);
    } catch (NumberFormatException e) {
      return def;
    }
    return result;
  }

  public static long getAsNumber(Properties args, String arg, long def) {
    long result = def;
    try {
      String s = args.getProperty(arg,"");
      if (!s.isEmpty())
        result = Long.parseLong(s);
    } catch (NumberFormatException e) {
      return def;
    }
    return result;
  }

  public static boolean getBoolean(Properties args, String arg) {
    return args.getProperty(arg,"false").equals("true");
  }

  static String colName(int colId, ValueArray ary   ) { return colName(colId,ary._cols[colId]._name); }
  static String colName(int colId, String n) { return n==null ? "Column "+colId : n; }

  static class InvalidInputException extends PageError {
    public InvalidInputException(String msg) {
      super(msg);
    }
  }

  static class InvalidColumnIdException extends InvalidInputException {
    public InvalidColumnIdException(String exp) {
      super("Invalid column identifier '" + exp + "'");
    }
  }

  public static int[] parseVariableExpression(ValueArray ary, String vexp) throws PageError {
    if( vexp.trim().isEmpty() ) return new int[0];
    String[] colExps = vexp.split(",");
    int[] res = new int[colExps.length];
    int idx = 0;
    __OUTER: for( int i = 0; i < colExps.length; ++i ) {
      String colExp = colExps[i].trim();
      if( colExp.contains(":") ) {
        String[] parts = colExp.split(":");
        if( parts.length != 2 ) throw new InvalidColumnIdException(colExp);
        int from = parseVariableExpression(ary, parts[0])[0];
        int to   = parseVariableExpression(ary, parts[1])[0];
        int[] new_res = new int[res.length + to - from];
        System.arraycopy(res, 0, new_res, 0, idx);
        for( int j = from; j <= to; ++j )
          new_res[idx++] = j;
        res = new_res;
        continue __OUTER;
      }
      res[idx++] = parseColumnNameOrIndex(ary, colExp);
    }
    return res;
  }

  public static int parseColumnNameOrIndex(ValueArray ary, String s) throws InvalidColumnIdException {
    s = s.trim();
    for( int j = 0; j < ary._cols.length; ++j )
      if( s.equalsIgnoreCase(ary._cols[j]._name) )
        return j;
    try {
      int i = Integer.valueOf(s);
      if( i < 0 || ary._cols.length <= i) throw new InvalidColumnIdException(s);
      return i;
    } catch( NumberFormatException e ) {
      throw new InvalidColumnIdException(s);
    }
  }

}
