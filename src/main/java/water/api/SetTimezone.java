package water.api;

import dontweave.gson.JsonObject;
import org.joda.time.DateTimeZone;
import water.*;
import water.api.RequestServer.API_VERSION;
import water.fvec.ParseTime;

import java.util.Set;

public class SetTimezone extends Func {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text for GET.
  static final String DOC_GET = "Set timezone to be used in parsing dates and times.";

  @API(help = "A string giving the name of the desired timezone.  For a list of acceptable names, use listTimezone().", required = true, filter = Default.class, json=true, gridable=false)
  public String tz;

  private static class SetTimezoneTask extends DRemoteTask {
    public String _tz;

    @Override
    public void lcompute() {
      ParseTime.setTimezone(_tz);
      tryComplete();
    }

    @Override public void reduce(DRemoteTask drt) {}
  }

  @Override protected void execImpl() {
    Set<String> idSet = DateTimeZone.getAvailableIDs();
    if(!idSet.contains(tz))
      throw new IllegalArgumentException("Unacceptable timezone name given.  For a list of acceptable names, use listTimezone().");

    SetTimezoneTask task = new SetTimezoneTask();
    task._tz = tz;
    task.invokeOnAllNodes();
  }

  // Reply value is the current setting
  @Override
  protected Response serve() {
    invoke();
    String s = ParseTime.getTimezone().getID();

    JsonObject response = new JsonObject();
    response.addProperty("tz", s);
    return Response.done(response);
  }

  @Override public API_VERSION[] supportedVersions() {
    return SUPPORTS_ONLY_V2;
  }
}
