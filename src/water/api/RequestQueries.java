package water.api;

import java.util.*;
import water.web.RString;

/**
 *
 * @author peta
 */
public class RequestQueries extends RequestArguments {

  /** Overwrite this method to be able to change / disable values of other
   * arguments on certain argument changes.
   *
   * This is done for both query checking and request checking.
   */
  protected void queryArgumentValueSet(Argument arg, Properties inputArgs) {

  }


  /** Checks the given arguments.
   *
   * When first argument is found wrong, generates the json error and returns the
   * result to be returned if any problems were found. Otherwise returns
   *
   * @param args
   * @param type
   * @return
   */
  protected final String checkArguments(Properties args, RequestType type) {
    // reset all arguments
    for (Argument arg: _arguments)
      arg.reset();
    // return query if in query mode
    if (type == RequestType.query)
      return buildQuery(args,type);
    // check the arguments now
    for (Argument arg: _arguments) {
      if (!arg.disabled()) {
        try {
          arg.check(args.getProperty(arg._name,""));
          queryArgumentValueSet(arg, args);
        } catch (IllegalArgumentException e) {
          if (type == RequestType.json)
            return jsonError("Argument "+arg._name+" error: "+e.getMessage()).toString();
          else
            return buildQuery(args,type);
        }
      }
    }
    return null;
  }

  protected static final String _queryHtml =
            "<div class='container'>"
          + "<div class='row-fluid'>"
          + "<div class='span12'>"
          + "<h3>Request %REQ_NAME ( <a href='%REQ_NAME.help'>help</a> )</h3>"
          + "<p></p>"
          + "  <dl class='dl-horizontal'><dt></dt><dd>"
          + "    <button class='btn btn-primary' onclick='query_submit()'>Submit</button>"
          + "    <button class='btn btn-info' onclick='query_refresh()'>Refresh</button>"
          + "    <button class='btn' onclick='query_reset()'>Reset</button>"
          + "  </dd></dl>"
          + "    %QUERY"
          + "  <dl class='dl-horizontal'><dt></dt><dd>"
          + "    <button class='btn btn-primary' onclick='query_submit()'>Submit</button>"
          + "    <button class='btn btn-info' onclick='query_refresh()'>Refresh</button>"
          + "    <button class='btn' onclick='query_reset()'>Reset</button>"
          + "  </dd></dl>"
          + "  <script type='text/javascript'>"
          + "    %SCRIPT"
          + "  </script>"
          + "</div></div></div>"
          ;

  private static final String _queryJs =
            "\nfunction query_refresh(event) {\n"
          + "  query_submit('.query', event.data, null);\n"
          + "}\n"
          + "function query_submit(requestType, specArg, specValue) {\n"
          + "  if (typeof(requestType) === 'undefined')\n"
          + "    requestType='.html';\n"
          + "  var request = {};\n"
          + "  %REQUEST_ELEMENT{"
          + "%ELEMENT_PREQ request.%ELEMENT_NAME = query_value_%ELEMENT_NAME();\n"
          + "  }\n"
          + "  var location = '%REQUEST_NAME'+requestType+'?'+$.param(request);\n"
          + "  window.location.replace(location);\n"
          + "}\n"
          + "function query_reset() {\n"
          + "  window.location.replace('%REQUEST_NAME.query');\n"
          + "}\n"
          + "%ELEMENT_VALUE{ %BODY\n }"
          + "%ELEMENT_ADDONS{ %BODY\n }"
          + "%ELEMENT_ONCHANGE{ %BODY\n }"
          ;


  /** Returns the request query form produced from the given input arguments.
   */
  protected String buildQuery(Properties args, RequestType type) {
    if (args.isEmpty())
      type = RequestType.query;
    RString result = new RString(_queryHtml);
    result.replace("REQ_NAME", this.getClass().getSimpleName());
    StringBuilder query = new StringBuilder();
    query.append("<form onsubmit='return false;'>");
    RString script = new RString(_queryJs);
    script.replace("REQUEST_NAME", getClass().getSimpleName());
    for (Argument arg: _arguments) {
      try {
        arg.check(args.getProperty(arg._name,""));
        queryArgumentValueSet(arg, args);
      } catch (IllegalArgumentException e) {
        // in query mode only display error for arguments present
        if ((type != RequestType.query) || !args.getProperty(arg._name,"").isEmpty())
          query.append("<div class='alert alert-error'>"+e.getMessage()+"</div>");
      }
      if (arg._hideInQuery)
        continue;
      query.append(arg.query());
      if (!arg.disabled()) {
        RString x = script.restartGroup("REQUEST_ELEMENT");
        x.replace("ELEMENT_NAME",arg._name);
        // If some Argument has prerequisites, and those pre-reqs changed on
        // this very page load then we do not assign the arg here: the values
        // passed will be something valid from the PRIOR page - based on the
        // old pre-req - and won't be correct.  Not assigning them here means
        // we'll act "as if" the field was never filled in.
        if( arg._prerequisites != null ) {
          StringBuilder sb = new StringBuilder("if( ");
          ArrayList<RequestArguments.Argument> preqs = arg._prerequisites;
          for( RequestArguments.Argument dep : preqs )
            sb.append("specArg!=='").append(dep._name).append("' && ");
          sb.append("true ) ");
          x.replace("ELEMENT_PREQ",sb);
        }
        x.append();
        x = script.restartGroup("ELEMENT_VALUE");
        x.replace("ELEMENT_NAME",arg._name);
        x.replace("BODY","function query_value_"+arg._name+"() { "+arg.jsValue()+"} ");
        x.append();
      }
      if (arg.refreshOnChange()) {
        RString x = script.restartGroup("ELEMENT_ONCHANGE");
        x.replace("BODY",arg.jsRefresh("query_refresh"));
        x.append();
      }
      RString x = script.restartGroup("ELEMENT_ADDONS");
      x.replace("BODY", arg.jsAddons());
      x.append();
    }
    query.append("</form>");
    result.replace("QUERY",query.toString());
    result.replace("SCRIPT",script.toString());
    return result.toString();
  }

}
