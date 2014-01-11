package water.api2;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import water.NanoHTTPD;
import water.api.RequestServer;
import java.util.ArrayList;

/**
 * Given an uri, list the files at that location.
 */
public class list_uri extends AbstractSimpleRequestHandler {
  public list_uri() {
    super(HML_GET, SINCE_V3, UNTIL_FOREVER, "/list_uri\\.json");
  }

  private static class UriEntry {
    public UriEntry(String u, long s) { uri = u; size = s;}

    public String uri;
    public long size;
  }

  private static class list_uri_Response {
    list_uri_Response() {
      uris = new ArrayList<UriEntry>();
      ignored_uris = new ArrayList<UriEntry>();
    }

    ArrayList<UriEntry> uris;
    ArrayList<UriEntry> ignored_uris;
  }

  public NanoHTTPD.Response serve(NanoHTTPD.IHTTPSession session) throws Exception {
    ParmHelper ph = new ParmHelper(session.getParms());
    String uri = ph.getRequiredStringParm("uri");
    String include_filter = ph.getOptionalStringParm("include_filter");
    String exclude_files = ph.getOptionalStringParm("exclude_files");
    ph.check();

    list_uri_Response response = new list_uri_Response();
    response.uris.add(new UriEntry("file://foo/bar", 10000));
    response.uris.add(new UriEntry("file://foo/bar/baz", 20000));
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String json = gson.toJson(response);

    NanoHTTPD.Response nr = new NanoHTTPD.Response(NanoHTTPD.Response.Status.OK, RequestServer.MIME_JSON, json);
    return nr;
  }
}
