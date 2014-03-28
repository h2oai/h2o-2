package water.api;

import water.Job;
import water.Key;
import water.api.RequestServer.API_VERSION;
import water.fvec.ParseDataset2;
import water.parser.CustomParser;
import water.util.RString;

public class Parse2 extends Parse {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="Response stats and info.") ResponseInfo response_info; // FIXME Parse2 should inherit from Request2

  @API(help = "Job key")
  public Key job_key; // Boolean read-only value; exists==>running, not-exists==>canceled/removed

  @API(help = "Destination key")
  public Key destination_key; // Key holding final value after job is removed

  @API(help="Drop source text from H2O memory after parsing")
  public Bool delete_on_done = new Bool("delete_on_done",true, "");

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
      Job parseJob = ParseDataset2.forkParseDataset(destination_key, keys, setup, delete_on_done.value());
      job_key = parseJob.self();
      // Allow the user to specify whether to block synchronously for a response or not.
      if (_blocking.value()) {
        parseJob.get(); // block until the end of job
        assert Job.isEnded(job_key) : "Job is still running but we already passed over its end. Job = " + job_key;
      }
      return Progress2.redirect(this,job_key,destination_key);
    } catch( Throwable e) {
      return Response.error(e);
    }
  }

  @Override public API_VERSION[] supportedVersions() { return SUPPORTS_ONLY_V2; }

  public void fillResponseInfo(Response response) {
    this.response_info = response.extractInfo();
  }
}
