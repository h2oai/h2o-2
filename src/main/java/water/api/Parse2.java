package water.api;

import water.Job;
import water.Key;
import water.api.Request.API;
import water.api.Request.Default;
import water.api.RequestServer.API_VERSION;
import water.fvec.ParseDataset2;
import water.parser.CustomParser;
import water.util.RString;

public class Parse2 extends Parse {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Job key")
  public Key job_key; // Boolean read-only value; exists==>running, not-exists==>canceled/removed

  @API(help = "Destination key")
  public Key destination_key; // Key holding final value after job is removed

  @API(help = "redirect to url")
  public String redirect_url; // Boolean read-only value; exists==>running, not-exists==>canceled/removed

  @API(help="Should block and wait for result?")
  protected Bool _blocking = new Bool("blocking",false, "");

  public static String link(String k, String content) {
    RString rs = new RString("<a href='Parse2.query?source_key=%key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }
  public Parse2(){_blocking._hideInQuery = true;}
  @Override protected Response serve() {
    PSetup p = _source.value();
    CustomParser.ParserSetup setup = p != null?p._setup._setup:new CustomParser.ParserSetup();
    setup._singleQuotes = _sQuotes.value();
    destination_key = Key.make(_dest.value());
    try {
      // Make a new Setup, with the 'header' flag set according to user wishes.
      Key[] keys = p._keys.toArray(new Key[p._keys.size()]);
      job_key = ParseDataset2.forkParseDataset(destination_key, keys, setup).job_key;
      redirect_url = Progress2.jsonUrl(job_key, destination_key);
      // Allow the user to specify whether to block synchronously for a response or not.
      if (_blocking.value())
        Job.waitUntilJobEnded(job_key);
      return Progress2.redirect(this,job_key,destination_key);
    } catch (IllegalArgumentException e) {
      return Response.error(e.getMessage());
    } catch (Error e) {
      return Response.error(e.getMessage());
    }
  }

  @Override public API_VERSION[] supportedVersions() { return SUPPORTS_ONLY_V2; }
}
