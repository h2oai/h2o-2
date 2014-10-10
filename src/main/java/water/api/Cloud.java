package water.api;

import dontweave.gson.*;
import water.*;
import water.util.Log;

import java.util.concurrent.ConcurrentHashMap;

public class Cloud extends Request2 {
  @API(help="quiet", required=false, filter=Default.class)
  protected boolean quiet = false;
  @API(help="skip_ticks", required=false, filter=Default.class)
  protected boolean skip_ticks = false;


  /**
   * Data structure to store last tick counts from a given node.
   */
  private class LastTicksEntry {
    final public long _system_idle_ticks;
    final public long _system_total_ticks;
    final public long _process_total_ticks;

    LastTicksEntry(HeartBeat hb) {
      _system_idle_ticks   = hb._system_idle_ticks;
      _system_total_ticks  = hb._system_total_ticks;
      _process_total_ticks = hb._process_total_ticks;
    }
  }

  @Override
  public RequestServer.API_VERSION[] supportedVersions() {
    return SUPPORTS_V1_V2;
  }

  /**
   * Store last tick counts for each node.
   *
   * This is local to a node and doesn't need to be Iced, so make it transient.
   * Access this each time the Cloud status page is called on this node.
   *
   * The window of tick aggregation is between calls to this page (which might come from the browser or from REST
   * API clients).
   *
   * Note there is no attempt to distinguish between REST API sessions.  Every call updates the last tick count info.
   */
  private static transient ConcurrentHashMap<String,LastTicksEntry> ticksHashMap = new ConcurrentHashMap<String, LastTicksEntry>();

  private static volatile boolean lastCloudHealthy = false;

  public Cloud() {
    _requestHelp = "Displays the information about the current cloud. For each"
            + " node displays its heartbeat information.";
  }

  @Override public Response serve() {
    JsonObject response = new JsonObject();
    final H2O cloud = H2O.CLOUD;
    final H2ONode self = H2O.SELF;
    response.addProperty(VERSION, H2O.VERSION);
    response.addProperty(CLOUD_NAME, H2O.NAME);
    response.addProperty(NODE_NAME, self.toString());
    response.addProperty(CLOUD_SIZE, cloud._memary.length);
    long now = System.currentTimeMillis();
    response.addProperty(CLOUD_UPTIME_MILLIS, now - H2O.START_TIME_MILLIS);
    boolean cloudHealthy = true;
    JsonArray nodes = new JsonArray();
    for (H2ONode h2o : cloud._memary) {
      HeartBeat hb = h2o._heartbeat;
      JsonObject node = new JsonObject();
      node.addProperty(NAME,h2o.toString());
      node.addProperty(NUM_KEYS, hb._keys);
      node.addProperty(VALUE_SIZE, hb.get_valsz());
      node.addProperty(FREE_MEM, hb.get_free_mem());
      node.addProperty(TOT_MEM, hb.get_tot_mem());
      node.addProperty(MAX_MEM, hb.get_max_mem());
      node.addProperty(MEM_BW, hb._membw);
      node.addProperty(FREE_DISK, hb.get_free_disk());
      node.addProperty(MAX_DISK, hb.get_max_disk());
      node.addProperty(NUM_CPUS, (int)hb._num_cpus);
      node.addProperty(GFLOPS, hb._gflops);
      node.addProperty(SYSTEM_LOAD, hb._system_load_average);

      Long elapsed = System.currentTimeMillis() - h2o._last_heard_from;
      node.addProperty(ELAPSED, elapsed);
      h2o._node_healthy = elapsed > HeartBeatThread.TIMEOUT ? false : true;
      node.addProperty(NODE_HEALTH, h2o._node_healthy);
      if (! h2o._node_healthy) {
        cloudHealthy = false;
      }
      node.addProperty("cpus_allowed", hb._cpus_allowed);
      node.addProperty("nthreads", hb._nthreads);
      node.addProperty("PID", hb._pid);

      JsonArray fjth = new JsonArray();
      JsonArray fjqh = new JsonArray();
      JsonArray fjtl = new JsonArray();
      JsonArray fjql = new JsonArray();
      if( hb._fjthrds != null ) {
        for( int i=0; i<H2O.MIN_HI_PRIORITY; i++ ) {
          if( hb._fjthrds[i]==-1 ) break;
          fjtl.add(new JsonPrimitive(hb._fjthrds[i]));
          fjql.add(new JsonPrimitive(hb._fjqueue[i]));
        }
        node.add(FJ_THREADS_LO, fjtl);
        node.add(FJ_QUEUE_LO  , fjql);
        for( int i=H2O.MIN_HI_PRIORITY; i<H2O.MAX_PRIORITY; i++ ) {
          fjth.add(new JsonPrimitive(hb._fjthrds[i]));
          fjqh.add(new JsonPrimitive(hb._fjqueue[i]));
        }
        node.add(FJ_THREADS_HI, fjth);
        node.add(FJ_QUEUE_HI  , fjqh);
      }

      node.addProperty(RPCS, (int) hb._rpcs);
      node.addProperty(TCPS_ACTIVE, (int) hb._tcps_active);
      if (hb._process_num_open_fds >= 0) { node.addProperty("open_fds", hb._process_num_open_fds); } else { node.addProperty("open_fds", "N/A"); }

      // Use tick information to calculate CPU usage percentage for the entire system and
      // for the specific H2O node.
      //
      // Note that 100% here means "the entire box".  This is different from 'top' 100%,
      // which usually means one core.
      int my_cpu_pct = -1;
      int sys_cpu_pct = -1;
      if (!skip_ticks) {
        LastTicksEntry lte = ticksHashMap.get(h2o.toString());
        if (lte != null) {
          long system_total_ticks_delta = hb._system_total_ticks - lte._system_total_ticks;

          // Avoid divide by 0 errors.
          if (system_total_ticks_delta > 0) {
            long system_idle_ticks_delta = hb._system_idle_ticks - lte._system_idle_ticks;
            double sys_cpu_frac_double = 1 - ((double)(system_idle_ticks_delta) / (double)system_total_ticks_delta);
            if (sys_cpu_frac_double < 0) sys_cpu_frac_double = 0;               // Clamp at 0.
            else if (sys_cpu_frac_double > 1) sys_cpu_frac_double = 1;          // Clamp at 1.
            sys_cpu_pct = (int)(sys_cpu_frac_double * 100);

            long process_total_ticks_delta = hb._process_total_ticks - lte._process_total_ticks;
            double process_cpu_frac_double = ((double)(process_total_ticks_delta) / (double)system_total_ticks_delta);
            // Saturate at 0 and 1.
            if (process_cpu_frac_double < 0) process_cpu_frac_double = 0;       // Clamp at 0.
            else if (process_cpu_frac_double > 1) process_cpu_frac_double = 1;  // Clamp at 1.
            my_cpu_pct = (int)(process_cpu_frac_double * 100);
          }
        }
        LastTicksEntry newLte = new LastTicksEntry(hb);
        ticksHashMap.put(h2o.toString(), newLte);
      }
      if (my_cpu_pct >= 0)  { node.addProperty("my_cpu_%", my_cpu_pct); }   else { node.addProperty("my_cpu_%", "N/A"); }
      if (sys_cpu_pct >= 0) { node.addProperty("sys_cpu_%", sys_cpu_pct); } else { node.addProperty("sys_cpu_%", "N/A"); }

      node.addProperty(LAST_CONTACT, h2o._last_heard_from);
      nodes.add(node);
    }

    response.addProperty(CLOUD_HEALTH, cloudHealthy);

    response.add(NODES,nodes);
    response.addProperty(CONSENSUS, Paxos._commonKnowledge); // Cloud is globally accepted
    response.addProperty(LOCKED, Paxos._cloudLocked); // Cloud is locked against changes

    boolean logCloudStatus = (!cloudHealthy) || (cloudHealthy != lastCloudHealthy) || !quiet;
    lastCloudHealthy = cloudHealthy;
    if (logCloudStatus) {
      Log.info("H2O Cloud Status:");
      for (String s : response.toString().split("[{}]"))
        if (!s.equals(",") && s.length() > 0) Log.info(s); // Log the cloud status to stdout
    }

    Response r = Response.done(response);
    r.setBuilder(CONSENSUS, new BooleanStringBuilder("","Voting new members"));
    r.setBuilder(LOCKED, new BooleanStringBuilder("Locked","Accepting new members"));
    r.setBuilder(NODES, new MyAryBuilder());
    r.setBuilder(NODES+"."+NAME, new NodeCellBuilder());
    r.setBuilder(NODES+"."+LAST_CONTACT, new LastContactBuilder());
    return r;
  }

  public static String pos_neg(double d) {
    return d >= 0 ? String.valueOf(d) : "n/a";
  }

  // Just the Node as a link
  private static class NodeCellBuilder extends ArrayRowElementBuilder {
    @Override public String elementToString(JsonElement element, String contextName) {
      String str = element.getAsString();
      if( str.equals(H2O.SELF.toString()) ) {
        return "<a href='StoreView.html'>"+str+"</a>";
      }

      String str2 = str.startsWith("/") ? str.substring(1) : str;
      String str3 = "<a href='http://" + str2 + "/StoreView.html'>" + str + "</a>";
      return str3;
    }
  }
  // Highlight sick nodes
  private static class MyAryBuilder extends ArrayBuilder {
    static ArrayRowBuilder MY_ARRAY_ROW = new MyRowBuilder();
    @Override public Builder defaultBuilder(JsonElement element) { return MY_ARRAY_ROW; }
  }
  private static class MyRowBuilder extends ArrayRowBuilder {
    @Override public String header(JsonObject object, String objectName) {
      long then = object.getAsJsonPrimitive(LAST_CONTACT).getAsLong();
      long now = System.currentTimeMillis();
      return ((now-then) >= HeartBeatThread.TIMEOUT) ? "\n<tr class=\"error\">" : "\n<tr>";
    }
  }
  // Last-heard-from time pretty-printing
  private static class LastContactBuilder extends ArrayRowElementBuilder {
    @Override public String elementToString(JsonElement element, String contextName) {
      long then = element.getAsLong();
      long now = System.currentTimeMillis();
      return (now-then >= 2*1000) ? ""+((now-then)/1000)+" secs ago" : "now";
    }
  }
}
