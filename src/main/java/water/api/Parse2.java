package water.api;

import water.Job;
import water.Key;
import water.fvec.ParseDataset2;
import water.parser.CustomParser;

public class Parse2 extends Parse {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="Should block and wait for result?")
  protected Bool _blocking = new Bool("blocking",false, "");

  public Parse2(){_blocking._hideInQuery = true;}
  @Override protected Response serve() {
    PSetup p = _source.value();
    CustomParser.ParserSetup setup = p._setup;
    Key d = Key.make(_dest.value());
    try {
      // Make a new Setup, with the 'header' flag set according to user wishes.
      Key[] keys = p._keys.toArray(new Key[p._keys.size()]);
      Key jobkey = ParseDataset2.forkParseDataset(d, keys, setup).job_key;
      // Allow the user to specify whether to block synchronously for a response or not.
      if (_blocking.value()) {
        Job.waitUntilJobEnded(jobkey);
      }
      return Progress2.redirect(this,jobkey,d);
    } catch (IllegalArgumentException e) {
      return Response.error(e.getMessage());
    } catch (Error e) {
      return Response.error(e.getMessage());
    }
  }
}
