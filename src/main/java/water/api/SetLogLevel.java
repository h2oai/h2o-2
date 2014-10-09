package water.api;

import water.*;
import water.api.RequestServer.API_VERSION;
import water.util.Log;

public class SetLogLevel extends Func {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text for GET.
  static final String DOC_GET = "Set runtime log4j log level";

  @API(help = "The new log level (1 == trace, 2 == debug, 3 == info, 4 == warn, 5 == error, 6 == fatal)", required = true, filter = Default.class, lmin = 1, lmax = 6)
  public int log_level = 1;

  private static class SetLogLevelTask extends DRemoteTask {
    public int _log_level;

    @Override
    public void lcompute() {
      Log.setLogLevel(_log_level);
      tryComplete();
    }

    @Override public void reduce(DRemoteTask drt) {}
  }

  @Override protected void execImpl() {
    SetLogLevelTask task = new SetLogLevelTask();
    task._log_level = log_level;
    task.invokeOnAllNodes();
  }

  @Override public API_VERSION[] supportedVersions() {
    return SUPPORTS_ONLY_V2;
  }
}
