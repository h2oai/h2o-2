package water.api;

import java.util.Date;

import water.Jobs.Job;
import water.Jobs.Progress;
import water.*;

import com.google.gson.*;

public class Jobs extends Request {
  Jobs() {
  }

  public static Response redirect(JsonObject resp, Key dest) {
    JsonObject redir = new JsonObject();
    redir.addProperty(KEY, dest.toString());
    return Response.redirect(resp, Jobs.class, redir);
  }

  @Override
  protected Response serve() {
    JsonObject result = new JsonObject();
    JsonArray array = new JsonArray();
    Job[] jobs = water.Jobs.get();
    for( int i = jobs.length - 1; i >= 0; i-- ) {
      JsonObject json = new JsonObject();
      json.addProperty(KEY, jobs[i]._key.toString());
      json.addProperty(DESCRIPTION, jobs[i]._description);
      json.addProperty(DEST_KEY, jobs[i]._dest != null ? jobs[i]._dest.toString() : "");
      json.addProperty(START_TIME, RequestBuilders.ISO8601.get().format(new Date(jobs[i]._startTime)));
      Progress progress = jobs[i]._progress != null ? UKV.get(jobs[i]._progress, new Progress()) : null;
      json.addProperty(PROGRESS, progress != null ? progress._value : 0f);
      json.addProperty(CANCELLED, water.Jobs.cancelled(jobs[i]._key));
      array.add(json);
    }
    result.add(JOBS, array);

    Response r = Response.done(result);
    r.setBuilder(JOBS, new ArrayBuilder() {
      @Override
      public String caption(JsonArray array, String name) {
        return "";
      }
    });
    r.setBuilder(JOBS + "." + KEY, new ArrayRowElementBuilder() {
      @Override
      public String elementToString(JsonElement elm, String contextName) {
        String html;
        if( water.Jobs.cancelled(Key.make(elm.getAsString())) )
          html = "<button disabled class='btn btn-mini'>X</button>";
        else {
          String keyParam = KEY + "=" + elm.getAsString();
          html = "<a href='Cancel.html?" + keyParam + "'><button class='btn btn-danger btn-mini'>X</button></a>";
        }
        return html;
      }
    });
    r.setBuilder(JOBS + "." + START_TIME, new ArrayRowElementBuilder() {
      @Override
      public String elementToString(JsonElement elm, String contextName) {
        return "<script>document.write(new Date(" + elm.toString() + ").toLocaleTimeString())</script>";
      }
    });
    r.setBuilder(JOBS + "." + PROGRESS, new ArrayRowElementBuilder() {
      @Override
      public String elementToString(JsonElement elm, String contextName) {
        return progress(Float.parseFloat(elm.getAsString()));
      }
    });
    return r;
  }

  private static String progress(float value) {
    String pct = "" + (int) (value * 100);
    // @formatter:off
    return ""
        + "<div style='margin-bottom:0px;padding-bottom:0xp;margin-top:8px;height:5px;width:180px' class='progress progress-stripped'>" //
          + "<div class='bar' style='width:" + pct + "%;'>" //
          + "</div>" //
        + "</div>";
    // @formatter:on
  }
}
