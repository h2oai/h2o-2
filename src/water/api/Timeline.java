
package water.api;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import water.*;
import water.util.TimelineSnapshot;
import water.web.RString;

import com.google.common.collect.Lists;
import com.google.gson.*;

public class Timeline extends Request {

  private static final String JSON_BYTES = "bytes";
  private static final String JSON_RECV = "recv";
  private static final String JSON_SEND = "send";
  private static final String JSON_NANOS = "nanos";
  private static final String JSON_TIME = "time";
  private static final String JSON_RECVS = "recvs";
  private static final String JSON_SENDS = "sends";
  private static final String JSON_CLOUD = "cloud";
  private static final String JSON_CLOUDS = "clouds";
  private static final String JSON_LAST_TIME = "lastTime";
  private static final String JSON_FIRST_TIME = "firstTime";
  private static final String JSON_TYPE = "type";
  private static final String JSON_EVENTS = "events";
  private static final String JSON_SELF = "self";
  private static final String JSON_NOW = "now";

  public Timeline() {
    _requestHelp = "Display a timeline of recent network traffic for debugging";
  }

  @Override public Response serve() {
    long ctm = System.currentTimeMillis();
    long[][] snapshot = TimeLine.system_snapshot();
    H2O cloud = TimeLine.CLOUD;
    TimelineSnapshot events = new TimelineSnapshot(cloud, snapshot);
    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss:SSS");

    JsonObject resJson = new JsonObject();
    resJson.addProperty(JSON_NOW, sdf.format(new Date(ctm)));
    resJson.addProperty(JSON_SELF, H2O.SELF.toString());

    JsonArray eventsJson = new JsonArray();
    resJson.add(JSON_EVENTS, eventsJson);

    List<TimelineSnapshot.Event> heartbeats = Lists.newArrayList();
    for( TimelineSnapshot.Event event : events ) {
      H2ONode h2o = cloud._memary[event.nodeId()];

      // The event type.  First get payload.
      long l0 = event.dataLo();
      long h8 = event.dataHi();
      int udp_type = (int)(l0&0xff); // First byte is UDP packet type
      UDP.udp e = UDP.udp.UDPS[udp_type];

      // Accumulate repeated heartbeats
      if( e == UDP.udp.heartbeat ){
        heartbeats.add(event);
        continue;
      }

      if( !heartbeats.isEmpty() ){
        long firstMs = heartbeats.get(0).ms();
        long lastMs = heartbeats.get(heartbeats.size()-1).ms();

        int totalSends = 0;
        int totalRecvs = 0;
        int[] sends = new int[cloud.size()];
        int[] recvs = new int[cloud.size()];
        for(TimelineSnapshot.Event h : heartbeats){
          if( h.isSend() ) {
            ++totalSends;
            ++sends[h.nodeId()];
          } else {
            ++totalRecvs;
            ++recvs[h.nodeId()];
          }
        }
        heartbeats.clear();

        JsonObject hbJson = new JsonObject();
        eventsJson.add(hbJson);

        hbJson.addProperty(JSON_TYPE, "heartbeat");
        hbJson.addProperty(JSON_FIRST_TIME, sdf.format(new Date(firstMs)));
        hbJson.addProperty(JSON_LAST_TIME, sdf.format(new Date(lastMs)));
        hbJson.addProperty(JSON_SENDS, totalSends);
        hbJson.addProperty(JSON_RECVS, totalRecvs);

        JsonArray cloudListJson = new JsonArray();
        hbJson.add(JSON_CLOUDS, cloudListJson);


        for( int i = 0; i < sends.length; ++i ) {
          JsonObject cloudJson = new JsonObject();
          cloudListJson.add(cloudJson);

          cloudJson.addProperty(JSON_CLOUD, TimeLine.CLOUD._memary[i].toString());
          cloudJson.addProperty(JSON_SENDS, sends[i]);
          cloudJson.addProperty(JSON_RECVS, recvs[i]);
        }
      }

      // Break down time into something readable
      long ms = event.ms(); // Event happened msec
      long ns = event.ns(); // Event happened nanosec

      String date = sdf.format(new Date(ms));

      JsonObject eventJson = new JsonObject();
      eventsJson.add(eventJson);

      eventJson.addProperty(JSON_TIME, date);
      eventJson.addProperty(JSON_NANOS, ns);
      eventJson.addProperty(JSON_TYPE, e.toString());

      if( event.isSend() ) {
        String recv = "multicast";
        InetAddress inet = event.addrPack();
        if( !inet.isMulticastAddress() ){
          int port = -1;
          if(events._sends.containsKey(event) && !events._sends.get(event).isEmpty())
            port = TimeLine.CLOUD._memary[events._sends.get(event).get(0).nodeId()]._key.getPort();
          String portStr = ":" + ((port != -1)?port:"?");

          recv = inet.toString() + portStr;
        }
        eventJson.addProperty(JSON_SEND, h2o.toString());
        eventJson.addProperty(JSON_RECV, recv);
      } else {
        eventJson.addProperty(JSON_SEND, event.addrString());
        eventJson.addProperty(JSON_RECV, h2o.toString());
      }
      eventJson.addProperty(JSON_BYTES, UDP.printx16(l0,h8));
    }
    Response r  = Response.done(resJson);
    r.setBuilder(JSON_EVENTS, new EventTableBuilder());
    return r;
  }

  private static class EventTableBuilder extends ArrayBuilder {
      @Override
      public String header(JsonArray array) {
        return "<table class='table table-striped table-bordered'>\n<thead>" +
            "<th>hh:mm:ss:ms</th>" +
            "<th>nanosec</th>" +
            "<th>who</th>" +
            "<th>event</th>" +
            "<th>bytes</th>" +
            "</thead>";
      }

      @Override
      public Builder defaultBuilder(JsonElement element) {
        JsonObject obj = (JsonObject)element;
        if( obj.get(JSON_TYPE).getAsString().equals("heartbeat") )
          return new HeartbeatEventRowBuilder();
        return new BasicEventRowBuild();
      }
  }
  private static class HeartbeatEventRowBuilder extends ArrayRowBuilder {
    @Override
    public String build(Response response, JsonObject object, String contextName) {
      String name = elementName(contextName);
      StringBuilder sb = new StringBuilder();
      sb.append(caption(object, name));
      sb.append(header(object, name));
      sb.append("<td>").append(object.get(JSON_LAST_TIME).getAsString()).append("</td>");
      sb.append("<td>lots</td>");
      sb.append("<td>many → many</td>");
      sb.append("<td>heartbeat</td>");
      sb.append("<td>");
      sb.append(object.get(JSON_SENDS).getAsLong()).append(" sends, ");
      sb.append(object.get(JSON_RECVS).getAsLong()).append(" recvs");
      sb.append("</td>");
      sb.append(footer(object, name));
      return sb.toString();
    }
  }
  private static class BasicEventRowBuild extends ArrayRowBuilder {
    @Override
    public String build(Response response, JsonObject object, String contextName) {
      String name = elementName(contextName);
      StringBuilder sb = new StringBuilder();
      sb.append(caption(object, name));
      sb.append(header(object, name));
      sb.append("<td>").append(object.get(JSON_TIME).getAsString()).append("</td>");
      sb.append("<td>").append(object.get(JSON_NANOS).getAsLong()).append("</td>");
      String s = object.get(JSON_SEND).getAsString();
      String r = object.get(JSON_RECV).getAsString();
      sb.append("<td>"+s+" → "+r+"</td>");
      sb.append("<td>").append(object.get(JSON_TYPE).getAsString()).append("</td>");
      sb.append("<td>").append(object.get(JSON_BYTES).getAsString()).append("</td>");
      sb.append(footer(object, name));
      return sb.toString();
    }
  }
}
