package water.api;

import water.api.RequestServer.API_VERSION;

import dontweave.gson.JsonArray;
import dontweave.gson.JsonObject;

public abstract class TypeaheadRequest extends Request {
  protected final Str _filter;
  protected final Int _limit;

  public TypeaheadRequest(String help, String filter) {
    _requestHelp = help;
    _filter = new Str(FILTER,filter);
    _filter._requestHelp = "Only items matching this filter will be returned.";
    _limit = new Int(LIMIT,1024,0,10240);
    _limit._requestHelp = "Max number of items to be returned.";
  }

  @Override final protected Response serve() {
    JsonArray array = serve(_filter.value(), _limit.value());
    JsonObject response = new JsonObject();
    response.add(ITEMS, array);
    return Response.done(response);
  }

  @Override protected boolean log() {
    return false;
  }

  abstract protected JsonArray serve(String filter, int limit);

  @Override public API_VERSION[] supportedVersions() { return SUPPORTS_V1_V2; }
}
