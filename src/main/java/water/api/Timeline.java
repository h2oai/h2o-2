package water.api;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import water.*;
import water.util.TimelineSnapshot;

import dontweave.gson.*;

public class Timeline extends Request {

  private static final String JSON_BYTES = "bytes";
  private static final String JSON_RECV = "recv";
  private static final String JSON_SEND = "send";
  private static final String JSON_DROP = "drop";
  private static final String JSON_NANOS = "nanos";
  private static final String JSON_TIME = "time";
  private static final String JSON_UDPTCP = "udp_tcp";
  private static final String JSON_RECVS = "recvs";
  private static final String JSON_SENDS = "sends";
  private static final String JSON_DROPS = "drops";
  private static final String JSON_CLOUD = "cloud";
  private static final String JSON_CLOUDS = "clouds";
  private static final String JSON_LAST_TIME = "lastTime";
  private static final String JSON_FIRST_TIME = "firstTime";
  private static final String JSON_TYPE = "type";
  private static final String JSON_SR = "sr";
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

    ArrayList<TimelineSnapshot.Event> heartbeats = new ArrayList();
    for( TimelineSnapshot.Event event : events ) {
      H2ONode h2o = cloud._memary[event._nodeId];

      // The event type.  First get payload.
      long l0 = event.dataLo();
      long h8 = event.dataHi();
      int udp_type = (int)(l0&0xff); // First byte is UDP packet type
      UDP.udp e = UDP.getUdp(udp_type);

      // Accumulate repeated heartbeats
      if( e == UDP.udp.heartbeat ) {
        heartbeats.add(event);
        continue;
      }

      // Now dump out accumulated heartbeats
      if( !heartbeats.isEmpty() ) {
        long firstMs = heartbeats.get(0).ms();
        long lastMs = heartbeats.get(heartbeats.size()-1).ms();

        int totalSends = 0;
        int totalRecvs = 0;
        int totalDrops = 0;
        int[] sends = new int[cloud.size()];
        int[] recvs = new int[cloud.size()];
        for(TimelineSnapshot.Event h : heartbeats){
          if( h.isSend() ) {
            ++totalSends;
            ++sends[h._nodeId];
          } else if( h.isDropped() ) {
            ++totalDrops;
          } else {
            ++totalRecvs;
            ++recvs[h._nodeId];
          }
        }
        heartbeats.clear();

        JsonObject hbJson = new JsonObject();
        eventsJson.add(hbJson);

        hbJson.addProperty(JSON_TYPE, "heartbeat");
        hbJson.addProperty(JSON_FIRST_TIME, sdf.format(new Date(firstMs)));
        hbJson.addProperty(JSON_LAST_TIME , sdf.format(new Date( lastMs)));
        hbJson.addProperty(JSON_SENDS, totalSends);
        hbJson.addProperty(JSON_RECVS, totalRecvs);
        hbJson.addProperty(JSON_DROPS, totalDrops);

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
      eventJson.addProperty(JSON_UDPTCP, event.ioflavor());
      eventJson.addProperty(JSON_TIME, date);
      eventJson.addProperty(JSON_NANOS, ns);
      eventJson.addProperty(JSON_TYPE, e.toString());
      eventJson.addProperty(JSON_SR, event.isSend());

      if( event.isSend() ) {
        eventJson.addProperty(JSON_SEND, h2o.toString());
        String recv = event.packH2O() == null ? "multicast" : event.packH2O().toString();
        eventJson.addProperty(JSON_RECV, recv);
      } else {
        eventJson.addProperty(JSON_SEND, event.packH2O().toString());
        eventJson.addProperty(JSON_RECV, h2o.toString());
        if( event.isDropped() )
          eventJson.addProperty(JSON_DROP, "1");
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
            "<th>I/O Kind</th>" +
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
      sb.append("<td>many -> many</td>");
      sb.append("<td>UDP</td>");
      sb.append("<td>heartbeat</td>");
      sb.append("<td>");
      sb.append(object.get(JSON_SENDS).getAsLong()).append(" sends, ");
      sb.append(object.get(JSON_RECVS).getAsLong()).append(" recvs, ");
      sb.append(object.get(JSON_DROPS).getAsLong()).append(" drops");
      sb.append("</td>");
      sb.append(footer(object, name));
      return sb.toString();
    }
  }
  private static class BasicEventRowBuild extends ArrayRowBuilder {
    @Override
    public String build(Response response, JsonObject object, String contextName) {
      StringBuilder sb = new StringBuilder();
      if( object.get(JSON_DROP) == null ) sb.append("<tr>");
      else sb.append("<tr style='background-color:Pink'>");
      sb.append("<td>").append(object.get(JSON_TIME).getAsString()).append("</td>");
      sb.append("<td>").append(object.get(JSON_NANOS).getAsLong()).append("</td>");
      boolean isSend = object.get(JSON_SR).getAsBoolean();
      String s = object.get(JSON_SEND).getAsString();
      String r = object.get(JSON_RECV).getAsString();
      sb.append("<td>");
      if(  isSend ) sb.append("<b>").append(s).append("</b>");
      else sb.append(s);
      sb.append(" -> ");
      if( !isSend ) sb.append("<b>").append(r).append("</b>");
      else sb.append(r);
      sb.append("</td>");
      sb.append("<td>").append(object.get(JSON_UDPTCP ).getAsString()).append("</td>");
      sb.append("<td>").append(object.get(JSON_TYPE ).getAsString()).append("</td>");
      sb.append("<td>").append(object.get(JSON_BYTES).getAsString()).append("</td>");
      sb.append("</tr>");
      return sb.toString();
    }
  }
}
