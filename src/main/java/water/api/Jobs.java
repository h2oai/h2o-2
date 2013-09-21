package water.api;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import water.Job;
import water.Key;

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
    Job[] jobs = Job.all();
    for( int i = jobs.length - 1; i >= 0; i-- ) {
      JsonObject json = new JsonObject();
      json.addProperty(KEY, jobs[i].self().toString());
      json.addProperty(DESCRIPTION, jobs[i].description);
      json.addProperty(DEST_KEY, jobs[i].dest() != null ? jobs[i].dest().toString() : "");
      json.addProperty(START_TIME, RequestBuilders.ISO8601.get().format(new Date(jobs[i].start_time)));
      long end = jobs[i].end_time;
      boolean cancelled = (end == 0 ? jobs[i].cancelled() : end == Job.CANCELLED_END_TIME);
      json.addProperty(END_TIME, end == 0 ? "" : RequestBuilders.ISO8601.get().format(new Date(end)));
      json.addProperty(PROGRESS, end == 0 ? (cancelled ? -2 : jobs[i].progress()) : (cancelled ? -2 : -1));
      json.addProperty(CANCELLED, cancelled);
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
        if( !Job.running(Key.make(elm.getAsString())) )
          html = "<button disabled class='btn btn-mini'>X</button>";
        else {
          String keyParam = KEY + "=" + elm.getAsString();
          html = "<a href='Cancel.html?" + keyParam + "'><button class='btn btn-danger btn-mini'>X</button></a>";
        }
        return html;
      }
    });
    r.setBuilder(JOBS + "." + DEST_KEY, new ArrayRowElementBuilder() {
      @Override
      public String elementToString(JsonElement elm, String contextName) {
        String str = elm.getAsString();
        String key = null;
        try {
          key = URLEncoder.encode(str,"UTF-8");
        } catch( UnsupportedEncodingException e ) { key = str; }
        return "".equals(key) ? key : "<a href='Inspect.html?"+KEY+"="+key+"'>"+str+"</a>";
      }
    });
    r.setBuilder(JOBS + "." + START_TIME, new ArrayRowElementBuilder() {
      @Override
      public String elementToString(JsonElement elm, String contextName) {
        return date(elm.toString());
      }
    });
    r.setBuilder(JOBS + "." + END_TIME, new ArrayRowElementBuilder() {
      @Override
      public String elementToString(JsonElement elm, String contextName) {
        return date(elm.toString());
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

  private static String date(String utc) {
    if( utc == null || utc.length() == 0 )
      return "";
    utc = utc.replaceAll("^\"|\"$","");
    Calendar cal = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss-SSSS");
    try {
        cal.setTime(sdf.parse(utc));
    } catch (Exception e) {
        System.out.println("Did not work! Message: "+e);
    }
    return "<script>document.write(new Date("+cal.get(Calendar.YEAR)+","+String.valueOf(cal.get(Calendar.MONTH)+1)+","+
            cal.get(Calendar.DAY_OF_MONTH)+","+cal.get(Calendar.HOUR_OF_DAY)+","+cal.get(Calendar.MINUTE)+","+cal.get(Calendar.SECOND)+").toLocaleTimeString())</script>";
    //return "<script>document.write(new Date(" + utc + ").toLocaleTimeString())</script>"; 
  }

  private static String progress(float value) {
    int    pct  = (int) (value * 100);
    String type = "progress-stripped active";
    if (pct==-100) { // task is done
      pct = 100;
      type = "progress-success";
    } else if (pct==-200) {
      pct = 100;
      type = "progress-warning";
    }

    // @formatter:off
    return ""
        + "<div style='margin-bottom:0px;padding-bottom:0xp;margin-top:8px;height:5px;width:180px' class='progress "+type+"'>" //
          + "<div class='bar' style='width:" + pct + "%;'>" //
          + "</div>" //
        + "</div>";
    // @formatter:on
  }
}
