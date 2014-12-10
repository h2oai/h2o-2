package water.api;

import dontweave.gson.JsonObject;
import water.*;
import water.api.RequestServer.API_VERSION;
import water.fvec.ParseTime;


public class ListTimezones extends Func {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text for GET.
  static final String DOC_GET = "Get name of current timezone setting used in parsing dates and times.";

  protected final Str _message = new Str("tzlist", "");

  @Override
  protected Response serve() {
    String s = ParseTime.listTimezones();

    JsonObject response = new JsonObject();
    response.addProperty("tzlist", s);
    return Response.done(response);
  }


  @Override public API_VERSION[] supportedVersions() {
    return SUPPORTS_ONLY_V2;
  }
}