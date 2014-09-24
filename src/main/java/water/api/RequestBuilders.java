package water.api;

import dontweave.gson.*;
import water.AutoBuffer;
import water.H2O;
import water.Iced;
import water.PrettyPrint;
import water.api.Request.API;
import water.api.RequestBuilders.Response.Status;
import water.util.JsonUtil;
import water.util.Log;
import water.util.RString;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** Builders and response object.
 *
 * It just has a stuff of simple builders that walk through the JSON response
 * and format the stuff into basic html. Understands simplest form of tables,
 * objects and elements.
 *
 * Also defines the response object that contains the response JSON, response
 * state, other response related automatic variables (timing, etc) and the
 * custom builders.
 *
 * TODO work in progress.
 *
 * @author peta
 */
public class RequestBuilders extends RequestQueries {
  public static final String ROOT_OBJECT  = "";
  public static final Gson   GSON_BUILDER = new GsonBuilder().setPrettyPrinting().create();

  private static final ThreadLocal<DecimalFormat> _format = new ThreadLocal<DecimalFormat>() {
    @Override protected DecimalFormat initialValue() {
      return new DecimalFormat("###.####");
    }
  };

  static final ThreadLocal<SimpleDateFormat> ISO8601 = new ThreadLocal<SimpleDateFormat>() {
    @Override protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    }
  };

  /** Builds the HTML for the given response.
   *
   * This is the root of the HTML. Should display all what is needed, including
   * the status, timing, etc. Then call the recursive builders for the
   * response's JSON.
   */
  protected String build(Response response) {
    StringBuilder sb = new StringBuilder();
    sb.append("<div class='container'>");
    sb.append("<div class='row-fluid'>");
    sb.append("<div class='span12'>");
    sb.append(buildJSONResponseBox(response));
    if( response._status == Response.Status.done ) response.toJava(sb);
    sb.append(buildResponseHeader(response));
    Builder builder = response.getBuilderFor(ROOT_OBJECT);
    if (builder == null) {
      sb.append("<h3>"+name()+"</h3>");
      builder = OBJECT_BUILDER;
    }
    for( String h : response.getHeaders() ) sb.append(h);
    if( response._response==null ) {
      boolean done = response._req.toHTML(sb);
      if(!done) {
        JsonParser parser = new JsonParser();
        String json = new String(response._req.writeJSON(new AutoBuffer()).buf());
        JsonObject o = (JsonObject) parser.parse(json);
        sb.append(builder.build(response, o, ""));
      }
    } else sb.append(builder.build(response,response._response,""));
    sb.append("</div></div></div>");
    return sb.toString();
  }

  protected String name() {
    return getClass().getSimpleName();
  }

  private static final String _responseHeader =
            "<table class='table table-bordered'><tr><td min-width: 60px><table style='font-size:12px;margin:0px;' class='table-borderless'>"
          + "  <tr>"
          + "    <td style='border:0px; min-width: 60px;' rowspan='2' style='vertical-align:top;'>%BUTTON&nbsp;&nbsp;</td>"
          + "    <td style='border:0px; min-width: 60px' colspan='6'>"
          + "      %TEXT"
          + "    </td>"
          + "  </tr>"
          + "  <tr>"
          + "    <td style='border:0px; min-width: 60px'><b>Cloud:</b></td>"
          + "    <td style='padding-right:70px;border:0px; min-width: 60px'>%CLOUD_NAME</td>"
          + "    <td style='border:0px; min-width: 60px'><b>Node:</b></td>"
          + "    <td style='padding-right:70px;border:0px; min-width: 60px'>%NODE_NAME</td>"
          + "    <td style='border:0px; min-width: 60px'><b>Time:</b></td>"
          + "    <td style='padding-right:70px;border:0px; min-width: 60px'>%TIME</td>"
          + "  </tr>"
          + "</table></td></tr></table>"
          + "<script type='text/javascript'>"
          + "%JSSTUFF"
          + "</script>"
          ;

  private static final String _redirectJs =
            "var timer = setTimeout('redirect()',000);\n"
          + "function countdown_stop() {\n"
          + "  clearTimeout(timer);\n"
          + "}\n"
          + "function redirect() {\n"
          + "  window.location.replace('%REDIRECT_URL');\n"
          + "}\n"
          ;

  private static final String _pollJs =
            "var timer = setTimeout(redirect,%TIMEOUT);\n"
          + "function countdown_stop() {\n"
          + "  clearTimeout(timer);\n"
          + "}\n"
          + "function redirect() {\n"
          + "  document.location.reload(true);\n"
          + "}\n"
          ;

  private static final String _jsonResponseBox =
            "<div class='pull-right'><a href='#' onclick='$(\"#json_box\").toggleClass(\"hide\");' class='btn btn-inverse btn-mini'>JSON</a></div>"
          + "<div class='hide' id='json_box'><pre><code class=\"language-json\">"
          + "%JSON_RESPONSE_BOX"
          + "</code></pre></div>";

  protected String buildJSONResponseBox(Response response) {
    switch (response._status) {
      case done    :
        RString result = new RString(_jsonResponseBox);
        result.replace("JSON_RESPONSE_BOX", response._response == null
                       ? new String(response._req.writeJSON(new AutoBuffer()).buf())
                       : GSON_BUILDER.toJson(response.toJson()));
        return result.toString();
      case error   :
      case redirect:
      case poll    :
      default      : return "";
    }
  }

  protected String buildResponseHeader(Response response) {
    RString result = new RString(_responseHeader);
    JsonObject obj = response.responseToJson();
    result.replace("CLOUD_NAME",obj.get(JSON_H2O).getAsString());
    result.replace("NODE_NAME",obj.get(NODE).getAsString());
    result.replace("TIME", PrettyPrint.msecs(obj.get(REQUEST_TIME).getAsLong(), true));
    switch (response._status) {
      case error:
        result.replace("BUTTON","<button class='btn btn-danger disabled'>"+response._status.toString()+"</button>");
        result.replace("TEXT","An error has occurred during the creation of the response. Details follow:");
        break;
      case done:
        //result.replace("BUTTON","<button class='btn btn-success disabled'>"+response._status.toString()+"</button>");
        //result.replace("TEXT","The result was a success and no further action is needed. JSON results are prettyprinted below.");
        result = new RString("");
        break;
      case redirect:
        result.replace("BUTTON","<button class='btn btn-primary' onclick='redirect()'>"+response._status.toString()+"</button>");
        result.replace("TEXT","Request was successful and the process was started. You will be redirected to the new page in 1 seconds, or when you click on the redirect"
                + " button on the left. If you want to keep this page for longer you can <a href='#' onclick='countdown_stop()'>stop the countdown</a>.");
        RString redirect = new RString(_redirectJs);
        redirect.replace("REDIRECT_URL",response._redirectName+".html"+encodeRedirectArgs(response._redirectArgs,response._redirArgs));
        result.replace("JSSTUFF", redirect.toString());
        break;
      case poll:
        if (response._redirectArgs != null) {
          RString poll = new RString(_redirectJs);
          poll.replace("REDIRECT_URL",requestName()+".html"+encodeRedirectArgs(response._redirectArgs,response._redirArgs));
          result.replace("JSSTUFF", poll.toString());
        } else {
          RString poll = new RString(_pollJs);
          poll.replace("TIMEOUT", response._pollProgress==0 ? 4500 : 5000);
          result.replace("JSSTUFF", poll.toString());
        }
        int pct = (int) ((double)response._pollProgress / response._pollProgressElements * 100);
        result.replace("BUTTON","<button class='btn btn-primary' onclick='redirect()'>"+response._status.toString()+"</button>");
        result.replace("TEXT","<div style='margin-bottom:0px;padding-bottom:0xp;height:5px;' class='progress progress-stripped'><div class='bar' style='width:"+pct+"%;'></div></div>"
                + "Request was successful, but the process has not yet finished.  The page will refresh every 5 seconds, or you can click the button"
                + " on the left.  If you want you can <a href='#' onclick='countdown_stop()'>disable the automatic refresh</a>.");
        break;
      default:
        result.replace("BUTTON","<button class='btn btn-inverse disabled'>"+response._status.toString()+"</button>");
        result.replace("TEXT","This is an unknown response state not recognized by the automatic formatter. The rest of the response is displayed below.");
        break;
    }
    return result.toString();
  }



  /** Basic builder for objects. ()
   */
  public static final Builder OBJECT_BUILDER = new ObjectBuilder();

  /** Basic builder for arrays. (table)
   */
  public static final Builder ARRAY_BUILDER = new ArrayBuilder();

  /** Basic builder for array rows. (tr)
   */
  public static final Builder ARRAY_ROW_BUILDER = new ArrayRowBuilder();

  /** Basic build for shaded array rows. (tr class='..')
   */
  public static final Builder ARRAY_HEADER_ROW_BUILDER = new ArrayHeaderRowBuilder();

  /** Basic builder for elements inside objects. (dl,dt,dd)
   */
  public static final ElementBuilder ELEMENT_BUILDER = new ElementBuilder();

  /** Basic builder for elements in array row objects. (td)
   */
  public static final Builder ARRAY_ROW_ELEMENT_BUILDER = new ArrayRowElementBuilder();

  /** Basic builder for elements in array rows single col. (tr and td)
   */
  public static final Builder ARRAY_ROW_SINGLECOL_BUILDER = new ArrayRowSingleColBuilder();

  // ===========================================================================
  // Response
  // ===========================================================================

  /** This is a response class for the JSON.
   *
   * Instead of simply returning a JsonObject, each request returns a new
   * response object that it must create. This is (a) cleaner (b) more
   * explicit and (c) allows to specify response states used for proper
   * error reporting, stateless and statefull processed and so on, and (d)
   * allows specification of HTML builder hooks in a nice clean interface.
   *
   * The work pattern should be that in the serve() method, a JsonObject is
   * created and populated with the variables. Then if any error occurs, an
   * error response should be returned.
   *
   * Otherwise a correct state response should be created at the end from the
   * json object and returned.
   *
   * JSON response structure:
   *
   * response :  status = (done,error,redirect, ...)
   *             h2o = name of the cloud
   *             node = answering node
   *             time = time in MS it took to process the request serve()
   *             other fields as per the response type
   * other fields that should go to the user
   * if error:
   * error :  error reported
   */
  public static final class Response {

    /** Status of the response.
     *
     * Defines the state of the response so that it can be nicely reported to
     * the user in either in JSON or in HTML in a meaningful manner.
     */
    public static enum Status {
      done, ///< Indicates that the request has completed and no further action from the user is required
      poll, ///< Indicates that the same request should be repeated to see some progress
      redirect, ///< Indicates that the request was successful, but new request must be filled to obtain results
      error ///< The request was an error.
    }

    /** Time it took the request to finish. In ms.
     */
    protected long _time;

    /** Status of the request.
     */
    protected final Status _status;

    /** Name of the redirected request. This is only valid if the response is
     * redirect status.
     */
    protected final String _redirectName;

    /** Arguments of the redirect object. These will be given to the redirect
     * object when called.
     */
    protected final JsonObject _redirectArgs;
    protected final Object[] _redirArgs;

    /** Poll progress in terms of finished elements.
     */
    protected final int _pollProgress;

    /** Total elements to be finished before the poll will be done.
     */
    protected final int _pollProgressElements;

    /** Response object for JSON requests.
     */
    protected final JsonObject _response;
    public final Request _req;

    protected boolean _strictJsonCompliance = true;

    /** Custom builders for JSON elements when converting to HTML automatically.
     */
    protected final HashMap<String,Builder> _builders = new HashMap();

    /** Custom headers to show in the html.
     */
    protected final List<String> _headers = new ArrayList();

    /** Private constructor creating the request with given type and response
     * JSON object.
     *
     * Use the static methods to construct the response objects. (looks better
     * when we have a lot of them).
     */
    private Response(Status status, JsonObject response) {
      _status = status;
      _response = response;
      _redirectName = null;
      _redirectArgs = null;
      _redirArgs = null;
      _pollProgress = -1;
      _pollProgressElements = -1;
      _req = null;
    }

    private Response(Status status, JsonObject response, String redirectName, JsonObject redirectArgs) {
      assert (status == Status.redirect);
      _status = status;
      _response = response;
      _redirectName = redirectName;
      _redirectArgs = redirectArgs;
      _redirArgs = null;
      _pollProgress = -1;
      _pollProgressElements = -1;
      _req = null;
    }

    private Response(Status status, JsonObject response, int progress, int total, JsonObject pollArgs) {
      assert (status == Status.poll);
      _status = status;
      _response = response;
      _redirectName = null;
      _redirectArgs = pollArgs;
      _redirArgs = null;
      _pollProgress = progress;
      _pollProgressElements = total;
      _req = null;
    }

    private Response(Status status, Request req, int progress, int total, Object...pollArgs) {
      assert (status == Status.poll);
      _status = status;
      _response = null;
      _redirectName = null;
      _redirectArgs = null;
      _redirArgs = pollArgs;
      _pollProgress = progress;
      _pollProgressElements = total;
      _req = req;
    }

    /** Response v2 constructor */
    private Response(Status status, Request req, int progress, int total, String redirTo, Object... args) {
      _status = status;
      _response = null;
      _redirectName = redirTo;
      _redirectArgs = null;
      _redirArgs = args;
      _pollProgress = progress;
      _pollProgressElements = total;
      _req = req;
    }


    /** Returns new error response with given error message.
     */
    public static Response error(Throwable e) {
      if( !(e instanceof IllegalAccessException ))
        Log.err(e);
      String message = e.getMessage();
      if( message == null ) message = e.getClass().toString();
      return error(message);
    }
    public static Response error(String message) {
      if( message == null ) message = "no error message";
      JsonObject obj = new JsonObject();
      obj.addProperty(ERROR,message);
      Response r = new Response(Status.error,obj);
      r.setBuilder(ERROR, new PreFormattedBuilder());
      return r;
    }

    /** Returns new done response with given JSON response object.
     */
    public static Response done(JsonObject response) {
      assert response != null : "Called Response.done with null JSON response - perhaps you should call Response.doneEmpty";
      return new Response(Status.done, response);
    }

    /** Response done v2. */
    public static Response done(Request req) {
      return new Response(Response.Status.done,req,-1,-1,(String) null);
    }

    /** A unique empty response which carries an empty JSON object */
    public static final Response EMPTY_RESPONSE = Response.done(new JsonObject());

    /** Returns new done empty done response.
     * Should be called only in cases which does not need json response.
     * see HTMLOnlyRequest
     */
    public static Response doneEmpty() {
      return EMPTY_RESPONSE;
    }

    /** Creates the new response with status redirect. This response will be
     * redirected to another request specified by redirectRequest with the
     * redirection arguments provided in redirectArgs.
     */
    public static Response redirect(JsonObject response,
        Class<? extends Request> req, JsonObject args) {
      return new Response(Status.redirect, response,
          req.getSimpleName(), args);
    }

    /** Redirect for v2 API */
    public static Response redirect(Request req, String redirectName, Object...redirectArgs) {
      return new Response(Response.Status.redirect, req, -1, -1, redirectName, redirectArgs);
    }

    /** Returns the poll response object.
     */
    public static Response poll(JsonObject response, int progress, int total) {
      return new Response(Status.poll,response, progress, total, null);
    }

    /** Returns the poll response object initialized by percents completed.
     */
    public static Response poll(JsonObject response, float progress) {
      int p = (int) (progress * 100);
      return Response.poll(response, p, 100);
    }

    /** returns the poll response object with different arguments that was
     * this call.
     */
    public static Response poll(JsonObject response, int progress, int total, JsonObject pollArgs) {
      return new Response(Status.poll,response, progress, total, pollArgs);
    }

    /** Returns the poll response object.
     */
    public static Response poll(Request req, int progress, int total, Object...pollArgs) {
      return new Response(Status.poll,req, progress, total, pollArgs);
    }

    /** Sets the time of the response as a difference between the given time and
     * now. Called automatically by serving request. Only available in JSON and
     * HTML.
     */
    public final void setTimeStart(long timeStart) {
      _time = System.currentTimeMillis() - timeStart;
    }

    /** Associates a given builder with the specified JSON context. JSON context
     * is a dot separated path to the JSON object/element starting from root.
     *
     * One exception is an array row element, which does not really have a
     * distinct name in JSON and is thus identified as the context name of the
     * array + "_ROW" appended to it.
     *
     * The builder object will then be called to build the HTML for the
     * particular JSON element. By wise subclassing of the preexisting builders
     * and changing their behavior an arbitrarily complex webpage can be
     * created.
     */
    public Response setBuilder(String contextName, Builder builder) {
      _builders.put(contextName, builder);
      return this;
    }

    /** Returns the builder for given JSON context element. Null if not found
     * in which case a default builder object will be used. These default
     * builders are specified by the builders themselves.
     */
    protected Builder getBuilderFor(String contextName) {
      return _builders.get(contextName);
    }

    public void addHeader(String h) { _headers.add(h); }

    public List<String> getHeaders() { return _headers; }


    /** Returns the response system json. That is the response type, time,
     * h2o basics and other automatic stuff.
     * @return
     */
    protected JsonObject responseToJson() {
      JsonObject resp = new JsonObject();
      resp.addProperty(STATUS,_status.toString());
      resp.addProperty(JSON_H2O, H2O.NAME);
      resp.addProperty(NODE, H2O.SELF.toString());
      resp.addProperty(REQUEST_TIME, _time);
      switch (_status) {
        case done:
        case error:
          break;
        case redirect:
          resp.addProperty(REDIRECT,_redirectName);
          if (_redirectArgs != null)
            resp.add(REDIRECT_ARGS,_redirectArgs);
          break;
        case poll:
          resp.addProperty(PROGRESS, _pollProgress);
          resp.addProperty(PROGRESS_TOTAL, _pollProgressElements);
          break;
        default:
          assert(false): "Unknown response type "+_status.toString();
      }
      return resp;
    }

    /** Returns the JSONified version of the request. At the moment just
     * returns the response.
     */
    public JsonObject toJson() {
      JsonObject res = _response;
      if( _strictJsonCompliance ) res = JsonUtil.escape(res);

      // in this case, creating a cyclical structure would kill us.
      if( _response != null && _response == _redirectArgs ) {
        res = new JsonObject();
        for( Entry<String, JsonElement> e : _response.entrySet() ) {
          res.add(e.getKey(), e.getValue());
        }
      }
      res.add(RESPONSE, responseToJson());
      return res;
    }

    public String toXml() {
      JsonObject jo = this.toJson();
      String jsonString = jo.toString();
      org.json.JSONObject jo2 = new org.json.JSONObject(jsonString);
      String xmlString = org.json.XML.toString(jo2);
      return xmlString;
    }

    public void toJava(StringBuilder sb) {
      if( _req != null ) _req.toJava(sb);
    }

     /** Returns the error of the request object if any. Returns null if the
     * response is not in error state.
     */
    public String error() {
      if (_status != Status.error)
        return null;
      return _response.get(ERROR).getAsString();
    }

    public void escapeIllegalJsonElements() {
      _strictJsonCompliance = true;
    }

    public ResponseInfo extractInfo() {
      String redirectUrl = null;
      if (_status == Status.redirect) redirectUrl = _redirectName+".json"+encodeRedirectArgs(_redirectArgs,_redirArgs);
      if (_status == Status.poll)     redirectUrl = _req.href()+".json"+encodeRedirectArgs(_redirectArgs,_redirArgs);
      return new ResponseInfo(redirectUrl, _time, _status);
    }
  }

  /** Class holding technical information about request/response. It will be served as a part of Request2's
   * response.
   */
  public static class ResponseInfo extends Iced {
    static final int API_WEAVER=1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    final @API(help="H2O cloud name.") String h2o;
    final @API(help="Node serving the response.") String node;
    final @API(help="Request processing time.") long time;
    final @API(help="Response status") Response.Status status;
    final @API(help="Redirect name.") String redirect_url;
    public ResponseInfo(String redirect_url, long time, Status status) {
      this.h2o = H2O.NAME;
      this.node = H2O.SELF.toString();
      this.redirect_url = redirect_url;
      this.time = time;
      this.status = status;
    }
  }

  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  /** An abstract class to build the HTML page automatically from JSON.
   *
   * The idea is that every JSON element in the response structure (dot
   * separated) may be a unique context that might be displayed in a different
   * way. By creating specialized builders and assigning them to the JSON
   * element contexts you can build arbitrarily complex HTML page.
   *
   * The basic builders for elements, arrays, array rows and elements inside
   * array rows are provided by default.
   *
   * Each builder can also specify default builders for its components to make
   * sure for instance that tables in arrays do not recurse and so on.
   */
  public static abstract class Builder {
    /** Override this method to provide HTML for the given json element.
     *
     * The arguments are the response object, the element whose HTML should be
     * produced and the contextName of the element.
     */
    public abstract String build(Response response, JsonElement element, String contextName);

    /** Adds the given element name to the existing context. Dot concatenates
     * the names.
     */
    public static String addToContext(String oldContext, String name) {
      if (oldContext.isEmpty())
        return name;
      return oldContext+"."+name;
    }

    /** For a given context returns the element name. That is the last word
     * after a dot, or the full string if dot is not present.
     */
    public static String elementName(String context) {
     int idx = context.lastIndexOf(".");
     return context.substring(idx+1);
    }

    /** Returns the default builders.
     *
     * These are element builder, object builder and array builder.
     */
    public Builder defaultBuilder(JsonElement element) {
      if (element instanceof JsonArray)
        return ARRAY_BUILDER;
      else if (element instanceof JsonObject)
        return OBJECT_BUILDER;
      else
        return ELEMENT_BUILDER;
    }

  }

  // ---------------------------------------------------------------------------
  // ObjectBuilder
  // ---------------------------------------------------------------------------

  /** Object builder.
   *
   * By default objects are displayed as a horizontal dl elements with their
   * heading preceding any of the values. Methods for caption, header,
   * footer as well as element building are provided so that the behavior can
   * easily be customized.
   */
  public static class ObjectBuilder extends Builder {

    /** Displays the caption of the object.
     */
    public String caption(JsonObject object, String objectName) {
      return objectName.isEmpty() ? "" : "<h4>"+objectName+"</h4>";
    }

    /** Returns the header of the object.
     *
     * That is any HTML displayed after caption and before any object's
     * contents.
     */
    public String header(JsonObject object, String objectName) {
      return "";
    }

    /** Returns the footer of the object.
     *
     * That is any HTML displayed after any object's contents.
     */
    public String footer(JsonObject object, String objectName) {
      return "";
    }

    /** Creates the HTML of the object.
     *
     * That is the caption, header, all its contents in order they were
     * added and then the footer. There should be no need to overload this
     * function, rather override the provided hooks above.
     */
    public String build(Response response, JsonObject object, String contextName) {
      StringBuilder sb = new StringBuilder();
      String name = elementName(contextName);
      sb.append(caption(object, name));
      sb.append(header(object, name));
      for (Map.Entry<String,JsonElement> entry : object.entrySet()) {
        JsonElement e = entry.getValue();
        String elementContext = addToContext(contextName, entry.getKey());
        Builder builder = response.getBuilderFor(elementContext);
        if (builder == null)
          builder = defaultBuilder(e);
        sb.append(builder.build(response, e, elementContext));
      }
      sb.append(footer(object, elementName(contextName)));
      return sb.toString();
    }

    /** The original build method. Calls build with json object, if not an
     * object, displays an alert box with the JSON contents.
     */
    public String build(Response response, JsonElement element, String contextName) {
      if (element instanceof JsonObject)
        return build(response, (JsonObject) element, contextName);
      return "<div class='alert alert-error'>Response element "+contextName+" expected to be JsonObject.  Automatic display not available</div><pre>"+element.toString()+"</pre>";
    }
  }

  public static class NoCaptionObjectBuilder extends ObjectBuilder {
    public String caption(JsonObject object, String objectName) { return ""; }
  }

  // ---------------------------------------------------------------------------
  // Array builder
  // ---------------------------------------------------------------------------

  /** Builds the HTML for an array. Arrays generally go to a table. Is similar
   * to the object, but rather than a horizontal dl generally displays as
   * a table.
   *
   * Can produce a header of the table and has hooks for rows.
   */
  public static class ArrayBuilder extends Builder {

    /** Caption of the table.
     */
    public String caption(JsonArray array, String name) {
      return "<h4>"+name+"</h4>";
    }

    /** Header of the table. Produces header off the first element if it is
     * object, or a single column header named value if it is a primitive. Also
     * includes the table tag.
     */
    public String header(JsonArray array) {
      StringBuilder sb = new StringBuilder();
      sb.append("<span style='display: inline-block;'>");
      sb.append("<table class='table table-striped table-bordered'>");
      if (array.get(0) instanceof JsonObject) {
        sb.append("<tr>");
        for (Map.Entry<String,JsonElement> entry : ((JsonObject)array.get(0)).entrySet())
          sb.append("<th style='min-width: 60px;'>").append(header(entry.getKey())).append("</th>");
        sb.append("</tr>");
      }
      return sb.toString();
    }

    public String header(String key) {
      return JSON2HTML(key);
    }

    /** Footer of the table, the end of table tag.
     */
    public String footer(JsonArray array) {
      return "</table></span>";
    }

    /** Default builders for the table. It is either a table row builder if the
     * row is an object, or a row single column builder if it is a primitive
     * or another array.
     */
    @Override public Builder defaultBuilder(JsonElement element) {
      return element instanceof JsonObject ? ARRAY_ROW_BUILDER : ARRAY_ROW_SINGLECOL_BUILDER;
    }

    /** Builds the array. Creates the caption, header, all the rows and the
     * footer or determines that the array is empty.
     */
    public String build(Response response, JsonArray array, String contextName) {
      StringBuilder sb = new StringBuilder();
      sb.append(caption(array, elementName(contextName)));
      if (array.size() == 0) {
        sb.append("<div class='alert alert-info'>empty array</div>");
      } else {
        sb.append(header(array));
        for (JsonElement e : array) {
          Builder builder = response.getBuilderFor(contextName+"_ROW");
          if (builder == null)
            builder = defaultBuilder(e);
          sb.append(builder.build(response, e, contextName));
        }
        sb.append(footer(array));
      }
      return sb.toString();
    }

    /** Calls the build method with array. If not an array, displays an alert
     * with the JSON contents of the element.
     */
    public String build(Response response, JsonElement element, String contextName) {
      if (element instanceof JsonArray)
        return build(response, (JsonArray)element, contextName);
      return "<div class='alert alert-error'>Response element "+contextName+" expected to be JsonArray. Automatic display not available</div><pre>"+element.toString()+"</pre>";
    }

  }

  // ---------------------------------------------------------------------------
  // ElementBuilder
  // ---------------------------------------------------------------------------

  /** A basic element builder.
   *
   * Elements are displayed as their string values, everything else as their
   * JSON values.
   */
  public static class ElementBuilder extends Builder {

    /** Displays the element in the horizontal dl layout. Override this method
     * to change the layout.
     */
    public String build(String elementContents, String elementName) {
      return "<dl class='dl-horizontal'><dt>"+elementName+"</dt><dd>"+elementContents+"</dd></dl>";
    }

    public String arrayToString(JsonArray array, String contextName) {
      return array.toString();
    }

    public String objectToString(JsonObject obj, String contextName) {
      return obj.toString();
    }

    public String elementToString(JsonElement elm, String contextName) {
        String elementName = elementName(contextName);
        if( elementName.endsWith(Suffixes.BYTES_PER_SECOND) ) {
          return PrettyPrint.bytesPerSecond(elm.getAsLong());
        } else if( elementName.endsWith(Suffixes.BYTES) ) {
          return PrettyPrint.bytes(elm.getAsLong());
        } else if( elementName.endsWith(Suffixes.MILLIS) ) {
          return PrettyPrint.msecs(elm.getAsLong(), true);
        } else if( elm instanceof JsonPrimitive && ((JsonPrimitive)elm).isString() ) {
          return elm.getAsString();
        } else if( elm instanceof JsonPrimitive && ((JsonPrimitive)elm).isNumber() ) {
          Number n = elm.getAsNumber();
          if( n instanceof Double ) {
            Double d = (Double) n;
            return format(d);
          }
          return elm.getAsString();
        } else {
          return elm.toString();
        }
    }

    public static String format(double value) {
      if( Double.isNaN(value) )
        return "";
      return _format.get().format(value);
    }

    public String elementToName(String contextName) {
      String base = elementName(contextName);
      for( String s : new String[] {
          Suffixes.BYTES_PER_SECOND,
          Suffixes.BYTES,
          Suffixes.MILLIS,
      }) {
        if( base.endsWith(s) )
          return base.substring(0, base.length() - s.length());
      }
      return base;
    }

    /** Based of the element type determines its string value and then calls
     * the string build version.
     */
    @Override public String build(Response response, JsonElement element, String contextName) {
      String base;
      if (element instanceof JsonArray) {
        base = arrayToString((JsonArray)element, contextName);
      } else if (element instanceof JsonObject) {
        base = objectToString((JsonObject)element, contextName);
      } else {
        base = elementToString(element, contextName);
      }
      return build(base, elementToName(contextName));
    }
  }

  public static class KeyElementBuilder extends ElementBuilder {
    @Override
    public String build(String content, String name) {
      try {
        String k = URLEncoder.encode(content, "UTF-8");
        return super.build("<a href='Inspect.html?key="+k+"'>"+content+"</a>", name);
      } catch( UnsupportedEncodingException e ) {
        throw Log.errRTExcept(e);
      }
    }
  }

  public static class PreFormattedBuilder extends ElementBuilder {
    @Override
    public String build(String content, String name) {
      return super.build("<pre>"+content+"</pre>", name);
    }
  }

  // Just the Key as a link, without any other cruft
  public static class KeyLinkElementBuilder extends ElementBuilder {
    @Override public String build(Response response, JsonElement element, String contextName) {
      try {
        String key = element.getAsString();
        String k = URLEncoder.encode(key, "UTF-8");
        return "<a href='Inspect.html?key="+k+"'>"+key+"</a>";
      } catch( UnsupportedEncodingException e ) {
        throw Log.errRTExcept(e);
      }
    }
  }

  public static class BooleanStringBuilder extends ElementBuilder {
    final String _t, _f;
    public BooleanStringBuilder(String t, String f) { _t=t; _f=f; }
    @Override public String build(Response response, JsonElement element, String contextName) {
      boolean b = element.getAsBoolean();
      return "<dl class='dl-horizontal'><dt></dt><dd>"+(b?_t:_f)+"</dd></dl>";
    }
  }
  public static class HideBuilder extends ElementBuilder {
    @Override public String build(Response response, JsonElement element, String contextName) {
      return "";
    }
  }


  // ---------------------------------------------------------------------------
  // ArrayRowBuilder
  // ---------------------------------------------------------------------------

  /** A row in the array table.
   *
   * Is an object builder with no caption and header and footer being the
   * table row tags. Default builder is array row element (td).
   */
  public static class ArrayRowBuilder extends ObjectBuilder {
    @Override public String caption(JsonObject object, String objectName) {
      return "";
    }

    @Override public String header(JsonObject object, String objectName) {
      //Gson g = new Gson();

      //Log.info(g.toJson(object));
      return "<tr id='row_"+object.get("row")+"'>";
    }

    @Override public String footer(JsonObject object, String objectName) {
      return "</tr>";
    }

    @Override public Builder defaultBuilder(JsonElement element) {
      return ARRAY_ROW_ELEMENT_BUILDER;
    }

  }

  public static class ArrayHeaderRowBuilder extends ArrayRowBuilder {
	@Override public String header(JsonObject object, String objectName) {
	  return "<tr class='warning'>";
	}
  }

  // ---------------------------------------------------------------------------
  // ArrayRowElementBuilder
  // ---------------------------------------------------------------------------

  /** Default array row element.
   *
   * A simple element builder than encapsulates into a td.
   */
  public static class ArrayRowElementBuilder extends ElementBuilder {
    public String build(String elementContents, String elementName) {
      return "<td style='min-width: 60px;'>"+elementContents+"</td>";
    }
  }

  // ---------------------------------------------------------------------------
  // ArrayRowSingleColBuilder
  // ---------------------------------------------------------------------------

  /** Array row for primitives.
   *
   * A row with single td element.
   */
  public static class ArrayRowSingleColBuilder extends ElementBuilder {
    public String build(String elementContents, String elementName) {
      return "<tr style='min-width: 60px;'><td style='min-width: 60px;'>"+elementContents+"</td></tr>";
    }
  }

  // ---------------------------------------------------------------------------
  // PaginatedTable
  // ---------------------------------------------------------------------------

  /** A table with pagination controls.
   *
   * Use this builder when large data is returned not at once.
   */

  public static class PaginatedTable extends ArrayBuilder {
    protected final String _offsetJSON;
    protected final String _viewJSON;
    protected final JsonObject _query;
    protected final long _max;
    protected final boolean _allowInfo;
    protected final long _offset;
    protected final int _view;

    public PaginatedTable(JsonObject query, long offset, int view, long max, boolean allowInfo, String offsetJSON, String viewJSON) {
      _offsetJSON = offsetJSON;
      _viewJSON = viewJSON;
      _query = query;
      _max = max;
      _allowInfo = allowInfo;
      _offset = offset;
      _view = view;
    }

    public PaginatedTable(JsonObject query, long offset, int view, long max, boolean allowInfo) {
      this(query, offset, view, max, allowInfo, OFFSET, VIEW);
    }

    protected String link(String caption, long offset, int view, boolean disabled) {
      _query.addProperty(_offsetJSON, offset);
      _query.addProperty(_viewJSON, view);
      if (disabled)
        return "<li class='disabled'><a>"+caption+"</a></li>";
      else
        return "<li><a href='"+RequestStatics.encodeRedirectArgs(_query,null)+"'>"+caption+"</a></li>";
    }

    protected String infoButton() {
      if (!_allowInfo)
        return "";
      return "<span class='pagination'><ul>"+link("info",-1,_view,_offset==1)+"</ul></span>&nbsp;&nbsp;";
    }


    protected String pagination() {
      StringBuilder sb = new StringBuilder();
      sb.append("<div style='text-align:center;'>");
      sb.append(infoButton());
      long firstPageItems = _offset % _view;
      long lastPageItems = (_max-_offset) % _view;
      long prevPages  = _offset / _view + (firstPageItems>0?1:0);
      long nextPages  = _offset + _view >= _max ? 0 : Math.max(_max-_offset-_view, 0) / _view + (lastPageItems>0?1:0);
      long lastOffset = _offset + nextPages * _view;
      long currentIdx = prevPages;
      long lastIdx = currentIdx+nextPages;
      long startIdx = Math.max(currentIdx-5,0);
      long endIdx = Math.min(startIdx + 11, lastIdx);
      if (_offset == -1)
        currentIdx = -1;

      sb.append("<span class='pagination'><ul>");
      sb.append(link("|&lt;",0,_view, _offset == 0));
      sb.append(link("&lt;",Math.max(_offset-_view,0),_view, currentIdx==0));
      if (startIdx>0)
        sb.append(link("...",0,0,true));
      for (long i = startIdx; i <= endIdx; ++i)
        sb.append(link(String.valueOf(i),_view*i,_view,i == currentIdx));
      if (endIdx<lastIdx)
        sb.append(link("...",0,0,true));
      sb.append(link("&gt;",_offset+_view,_view, currentIdx == lastIdx));
      sb.append(link("&gt;|",lastOffset,_view, currentIdx == lastIdx));
      sb.append("</ul></span>");
      sb.append("</div>");
      return sb.toString();
    }

    @Override public String header(JsonArray array) {
      StringBuilder sb = new StringBuilder();
      sb.append(pagination());
      sb.append(super.header(array));
      return sb.toString();
    }

    @Override public String footer(JsonArray array) {
      StringBuilder sb = new StringBuilder();
      sb.append(super.footer(array));
      sb.append(pagination());
      return sb.toString();
    }

  }

  public class WarningCellBuilder extends ArrayRowElementBuilder {
    @Override public String arrayToString(JsonArray arr, String contextName) {
      StringBuilder sb = new StringBuilder();
      String sep = "";
      for( JsonElement e : arr ) {
        sb.append(sep).append(e.getAsString());
        sep = "</br>";
      }
      return sb.toString();
    }
  }

  public class KeyCellBuilder extends ArrayRowElementBuilder {
    @Override public String elementToString(JsonElement element, String contextName) {
      String str = element.getAsString();
      try {
        String key = URLEncoder.encode(str,"UTF-8");
        String delete = "<a href='RemoveAck.html?"+KEY+"="+key+"'><button class='btn btn-danger btn-mini'>X</button></a>";
        return delete + "&nbsp;&nbsp;" + Inspector.link(str, str);
      } catch( UnsupportedEncodingException e ) {
        throw  Log.errRTExcept(e);
      }
    }
  }

  public class KeyMinAvgMaxBuilder extends ArrayRowElementBuilder {
    private String trunc(JsonObject obj, String fld, int n) {
      JsonElement je = obj.get(fld);
      if( je == null || je instanceof JsonNull ) return "<br>";
      String s1 = je.getAsString();
      String s2 = (s1.length() > n ?  s1.substring(0,n) : s1);
      String s3 = s2.replace(" ","&nbsp;");
      return s3+"<br>";
    }
    @Override public String objectToString(JsonObject obj, String contextName) {
      if (!obj.has(MIN)) return "";
      return "<strong>"+trunc(obj,HEADER,10)+"</strong>"+trunc(obj,MIN,6)+trunc(obj,MEAN,6)+trunc(obj,MAX,6);
    }
  }

}
