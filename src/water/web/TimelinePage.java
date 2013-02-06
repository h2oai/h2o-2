package water.web;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import water.*;
import water.UDP.udp;
import water.util.TimelineSnapshot;

public class TimelinePage extends H2OPage {

  @Override
  public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    long ctm = System.currentTimeMillis();

    long[][] snapshot = TimeLine.system_snapshot();
    H2O cloud = TimeLine.CLOUD;
    TimelineSnapshot events = new TimelineSnapshot(cloud, snapshot);
    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss:SSS");

    JsonObject resJson = new JsonObject();
    resJson.addProperty("now", sdf.format(new Date(ctm)));
    resJson.addProperty("self", H2O.SELF.toString());

    JsonArray eventsJson = new JsonArray();
    resJson.add("events", eventsJson);

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

        int[] sends = new int[cloud.size()];
        int[] recvs = new int[cloud.size()];
        for(TimelineSnapshot.Event h : heartbeats){
          ++(h.isSend() ? sends: recvs)[h.nodeId()];
        }
        heartbeats.clear();

        JsonObject hbJson = new JsonObject();
        eventsJson.add(hbJson);

        hbJson.addProperty("type", "heartbeat");
        hbJson.addProperty("firstTime", sdf.format(new Date(firstMs)));
        hbJson.addProperty("lastTime", sdf.format(new Date(lastMs)));

        JsonArray cloudListJson = new JsonArray();
        hbJson.add("clouds", cloudListJson);

        for( int i = 0; i < sends.length; ++i ) {
          JsonObject cloudJson = new JsonObject();
          cloudListJson.add(cloudJson);

          cloudJson.addProperty("cloud", TimeLine.CLOUD._memary[i].toString());
          cloudJson.addProperty("sends", sends[i]);
          cloudJson.addProperty("recvs", sends[i]);
        }
      }

      // Break down time into something readable
      long ms = event.ms(); // Event happened msec
      long ns = event.ns(); // Event happened nanosec

      String date = sdf.format(new Date(ms));

      JsonObject eventJson = new JsonObject();
      eventsJson.add(eventJson);

      eventJson.addProperty("type", e.toString());
      eventJson.addProperty("time", date);
      eventJson.addProperty("nanos", ns);

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
        eventJson.addProperty("send", h2o.toString());
        eventJson.addProperty("recv", recv);
      } else {
        eventJson.addProperty("send", event.addrString());
        eventJson.addProperty("recv", h2o.toString());
      }
      eventJson.addProperty("bytes", UDP.printx16(l0,h8));
    }

    return resJson;
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    long ctm = System.currentTimeMillis();

    // Take a system-wide snapshot
    long[][] snapshot = TimeLine.system_snapshot();
    TimelineSnapshot events = new TimelineSnapshot(TimeLine.CLOUD, snapshot);
    H2O cloud = TimeLine.CLOUD;
    RString response = new RString(html);

    // Get the most recent event time
    response.replace("now",new SimpleDateFormat("yyyy.MM.dd HH:mm").format(new Date(ctm)));
    response.replace("self",H2O.SELF);

    // some pretty ways to view time
    SimpleDateFormat sdf0 = new SimpleDateFormat("HH:mm:ss:SSS");
    SimpleDateFormat sdf1 = new SimpleDateFormat(":SSS");

    // We have a system-wide snapshot: timelines from each Node.  We will be
    // picking one event from all our various timelines at a time.  This means
    // we need a Cursor - a pointer into EACH timeline, and we'll pick and
    // advance one at a time.


    // Build a time-sorted table of events
    int alt=0;                  // Alternate color per row
    long sec = 0;               // Last second viewed
    long nsec = 0;              // Last nanosecond viewed
    ArrayList<TimelineSnapshot.Event> heartbeats = new ArrayList<TimelineSnapshot.Event> ();
    for(TimelineSnapshot.Event event:events){
      H2ONode h2o = cloud._memary[event.nodeId()];

      // The event type.  First get payload.
      long l0 = event.dataLo();
      long h8 = event.dataHi();
      int udp_type = (int)(l0&0xff); // First byte is UDP packet type
      UDP.udp e = UDP.udp.UDPS[udp_type];

      InetAddress inet = event.addrPack();

      // See if this is a repeated Heartbeat.
      if( e == UDP.udp.heartbeat ){
        heartbeats.add(event);
        continue;
      } else if(!heartbeats.isEmpty()){
        int [] sends = new int [TimeLine.CLOUD.size()];
        int [] recvs = new int [TimeLine.CLOUD.size()];
        for(TimelineSnapshot.Event h:heartbeats){
          if(h.isSend()) ++sends[h.nodeId()]; else ++recvs[h.nodeId()];
        }
        StringBuilder heartBeatStr = new StringBuilder();
        int allSends = 0;
        int allRecvs = 0;
        for(int i = 0; i < sends.length; ++i){
          if(i != 0)heartBeatStr.append(", ");
          heartBeatStr.append(sends[i] + ":" + recvs[i]);
          allSends += sends[i];
          allRecvs += recvs[i];
        }
        long hms = heartbeats.get(heartbeats.size()-1).ms(); // Event happened msec
        long hsec0 = hms/1000;
        String hdate = ((hsec0 == sec) ? sdf1 : sdf0).format(new Date(hms));
        sec = hsec0;
        RString row = response.restartGroup("tableRow");
        row.replace("udp","heartbeat");
        row.replace("msec",hdate);
        row.replace("nsec","lots");
        row.replace("send","many");
        row.replace("recv","many");
        row.replace("bytes", allSends + " sends, " + allRecvs + " recvs (" + heartBeatStr.toString() + ")");
        row.append();
        heartbeats.clear();
      }

      // Break down time into something readable
      long ms = event.ms(); // Event happened msec
      long ns = event.ns(); // Event happened nanosec
      long sec0 = ms/1000;         // Round down to nearest second
      String date = ((sec0 == sec) ? sdf1 : sdf0).format(new Date(ms));
      sec = sec0;

      // A row for this event
      RString row = response.restartGroup("tableRow");

      row.replace("udp",e.toString());
      row.replace("msec",date);
      row.replace("nsec",((Math.abs(ns-nsec)>2000000)?"lots":(ns-nsec)));
      nsec = ns;

      // Who and to/from
      if( event.isSend()) { // This is a SENT packet
        row.replace("send","<strong>"+h2o+"</strong>"); // sent from self
        if(!inet.isMulticastAddress()){
          int port = -1;
          if(events._sends.containsKey(event) && !events._sends.get(event).isEmpty())
            port = TimeLine.CLOUD._memary[events._sends.get(event).get(0).nodeId()]._key.getPort();
          String portStr = ":" + ((port != -1)?port:"?");//((port != -1)?port:"?");
          String addrString = inet.toString() + portStr;
          row.replace("recv",addrString);
        } else
          row.replace("recv","multicast");
      } else { // Else this is a RECEIVED packet
        // get the sender's port
        int port = event.portPack();
        row.replace("send",event.addrString());
        row.replace("recv","<strong>"+((inet.equals(h2o._key) && (port == h2o._key.getPort()))?"self":h2o)+"</strong>");
      }
      if( e != UDP.udp.bad) row.replace("bytes",UDP.printx16(l0,h8));
      row.append();
    }

    response.replace("noOfRows",alt);
    return response.toString();
  }

  final static String html =
           "<div class='alert alert-success'>Snapshot taken: <strong>%now</strong> by <strong>%self</strong></div>"
          + "<p>Showing %noOfRows events\n"
          + "<table class='table table-striped table-bordered table-condensed'>"
          + "<thead><th>hh:mm:ss:ms<th>nanosec<th>who<th>event<th>bytes</thead>\n"
          + "<tbody>"
          + "%tableRow{"
          + "  <tr>"
          + "    <td align=right>%msec</td>"
          + "    <td align=right>+%nsec</td>"
          + "    <td>%send&rarr;%recv</td>"
          + "    <td>%udp</td>"
          + "    <td>%bytes</td>"
          + "  </tr>\n"
          + "}"
          + "</table>"
          ;
}
