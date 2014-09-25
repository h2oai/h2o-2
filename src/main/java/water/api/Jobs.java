package water.api;

import dontweave.gson.JsonArray;
import dontweave.gson.JsonElement;
import dontweave.gson.JsonObject;
import water.DKV;
import water.Job;
import water.Job.JobState;
import water.Key;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.ParseException;
import java.util.Date;

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
      JsonObject jobResult = new JsonObject();
      Job job = jobs[i];

      boolean cancelled;
      if (cancelled = (job.state==JobState.CANCELLED || job.state==JobState.FAILED)) {
        if(job.exception != null){
          jobResult.addProperty("exception", "1");
          jobResult.addProperty("val", jobs[i].exception);
        } else {
          jobResult.addProperty("val", "CANCELLED");
        }
      } else if (job.state==JobState.DONE)
        jobResult.addProperty("val", "OK");
      json.addProperty(END_TIME, end == 0 ? "" : RequestBuilders.ISO8601.get().format(new Date(end)));
      json.addProperty(PROGRESS, job.state==JobState.RUNNING || job.state==JobState.DONE ? jobs[i].progress() : -1);
      json.addProperty(PROGRESS, end == 0 ? (cancelled ? -2 : jobs[i].progress()) : (cancelled ? -2 : -1));
      json.addProperty(CANCELLED, cancelled);
      json.add("result",jobResult);
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
        if( !Job.isRunning(Key.make(elm.getAsString())) )
          html = "<button disabled class='btn btn-mini'>X</button>";
        else {
          String keyParam = KEY + "=" + elm.getAsString();
          html = "<a href='/Cancel.html?" + keyParam + "'><button class='btn btn-danger btn-mini'>X</button></a>";
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
        return ("".equals(key) || DKV.get(Key.make(str)) == null) ? key : Inspector.link(str, str);
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
    r.setBuilder(JOBS + "." + "result", new ElementBuilder() {
      @Override
      public String objectToString(JsonObject obj, String contextName) {
        if(obj.has("exception")){
          String rid = Key.make().toString();
          String ex = obj.get("val").getAsString().replace("'", "");
          String [] lines = ex.split("\n");
          StringBuilder sb = new StringBuilder(lines[0]);
          for(int i = 1; i < lines.length; ++i){
            sb.append("\\n" + lines[i]);
          }
//          ex = ex.substring(0,ex.indexOf('\n'));
          ex = sb.toString();
          String res =  "\n<a onClick=\"" +
              "var showhide=document.getElementById('" + rid + "');" +
              "if(showhide.innerHTML == '') showhide.innerHTML = '<pre>" + ex + "</pre>';" +
              "else showhide.innerHTML = '';" +
              "\">FAILED</a>\n<div id='"+ rid +"'></div>\n";
          return res;
        } else if(obj.has("val")){
          return obj.get("val").getAsString();
        }
        return "";
      }
      @Override
      public String build(String elementContents, String elementName) {
        return "<td>" + elementContents + "</td>";
      }
    });

    return r;
  }

  private static String date(String utc) {
    if( utc == null || utc.length() == 0 )
      return "";
    utc = utc.replaceAll("^\"|\"$","");
    if( utc.length() == 0 )
      return "";
    long ms;
    try {
      ms = RequestBuilders.ISO8601.get().parse(utc).getTime();
    } catch( ParseException e ) {
      throw new RuntimeException(e);
    }
    return "<script>document.write(new Date(" + ms + ").toLocaleTimeString())</script>";
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

  @Override
  public RequestServer.API_VERSION[] supportedVersions() {
    return SUPPORTS_V1_V2;
  }
}
