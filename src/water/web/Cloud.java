package water.web;

import com.google.gson.JsonObject;
import java.util.Properties;
import water.*;

public class Cloud extends H2OPage {

  public Cloud() {
    _refresh = 5;
  }

  @Override
  public JsonObject serverJson(Server server, Properties parms, String sessionID) {
    JsonObject res = new JsonObject();
    final H2O cloud = H2O.CLOUD;
    final H2ONode self = H2O.SELF;
    HeartBeat hb = self._heartbeat;
    res.addProperty("cloud_name", H2O.NAME);
    res.addProperty("node_name",  self.toString());
    res.addProperty("cloud_size", cloud._memary.length);
    res.addProperty("consensus",  Paxos._commonKnowledge); // Cloud is globally accepted
    res.addProperty("locked",     Paxos._cloudLocked); // Cloud is locked against changes
    res.addProperty("fjthrds_hi", hb._fjthrds_hi);
    res.addProperty("fjqueue_hi", hb._fjqueue_hi);
    res.addProperty("fjthrds_lo", hb._fjthrds_lo);
    res.addProperty("fjqueue_lo", hb._fjqueue_lo);
    res.addProperty("rpcs"      , hb._rpcs);
    return res;
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    RString response = new RString(html);
    response.replace("cloud_name",H2O.NAME);
    response.replace("node_name",H2O.SELF.toString());
    final H2O cloud = H2O.CLOUD;
    for( H2ONode h2o : cloud._memary ) {
      HeartBeat hb = h2o._heartbeat;
      // restart the table line
      RString row = response.restartGroup("tableRow");
      // This hangs on ipv6 name resolution
      //String name = h2o._inet.getHostName();
      row.replace("host",h2o);
      row.replace("node",h2o);
      row.replace("num_cpus" ,            (int)(hb._num_cpus));
      row.replace("free_mem" ,PrettyPrint.bytes(hb.get_free_mem()));
      row.replace("tot_mem"  ,PrettyPrint.bytes(hb.get_tot_mem  ()));
      row.replace("max_mem"  ,PrettyPrint.bytes(hb.get_max_mem  ()));
      row.replace("num_keys" ,                 (hb._keys));
      row.replace("val_size" ,PrettyPrint.bytes(hb.get_valsz    ()));
      row.replace("free_disk",PrettyPrint.bytes(hb.get_free_disk()));
      row.replace("max_disk" ,PrettyPrint.bytes(hb.get_max_disk ()));

      row.replace("cpu_util" ,pos_neg(hb.get_cpu_util()));

      row.replace("cpu_load_1" ,pos_neg(hb.get_cpu_load1()));
      row.replace("cpu_load_5" ,pos_neg(hb.get_cpu_load5()));
      row.replace("cpu_load_15",pos_neg(hb.get_cpu_load15()));

      int fjq_hi = hb._fjqueue_hi;
      int fjt_hi = hb._fjthrds_hi;
      if(fjq_hi > HeartBeatThread.QUEUEDEPTH)
        row.replace("queueStyleHi","background-color:green;");
      row.replace("fjthrds_hi",  fjt_hi);
      row.replace("fjqueue_hi",  fjq_hi);
      int fjq_lo = hb._fjqueue_lo;
      int fjt_lo = hb._fjthrds_lo;
      if(fjq_lo > HeartBeatThread.QUEUEDEPTH)
        row.replace("queueStyleLo","background-color:green;");
      row.replace("fjthrds_lo",  fjt_lo);
      row.replace("fjqueue_lo",  fjq_lo);
      row.replace("rpcs",        (int)hb._rpcs);
      row.replace("tcps_active", (int)hb._tcps_active);

      row.append();
    }
    response.replace("size",cloud._memary.length);
    response.replace("voting",Paxos._commonKnowledge?"":"Voting in progress");
    response.replace("locked",Paxos._cloudLocked?"Cloud locked":"");
    return response.toString();
  }

  static String pos_neg( double d ) {
    return d>=0 ? Double.toString(d) : "n/a";
  }

  private final String html =
    "<div class='alert alert-success'>"
    + "You are connected to cloud <strong>%cloud_name</strong> and node <strong>%node_name</strong>."
    + "</div>\n"
    + "<p>The Local Cloud has %size members\n"
    + "<table class='table table-striped table-bordered table-condensed'>\n"
    + "<thead class=''><tr>\n"
    +     "<th rowspan=\"2\">Local Nodes</th>\n"
    +     "<th rowspan=\"2\">CPUs</th>\n"
    +     "<th rowspan=\"2\">Local Keys</th>\n"
    +     "<th colspan=\"4\" style='text-align:center'>Memory</th>\n"
    +     "<th colspan=\"2\" style='text-align:center'>Disk</th>\n"
    +     "<th colspan=\"4\" style='text-align:center'>CPU Load</th>\n"
    +     "<th colspan=\"3\" style='text-align:center'>Threads / Tasks</th>\n"
    +     "<th rowspan=\"2\">TCPs Active</th>\n"
    + "</tr>\n"
    + "<tr>\n"
    +     "<th>Cached</th>\n"  // memory
    +     "<th>Free</th>\n"
    +     "<th>Total</th>\n"
    +     "<th>Max</th>\n"
    +     "<th>Free</th>\n"    // disk
    +     "<th>Max</th>\n"
    +     "<th>Util</th>\n"    // CPU
    +     "<th>1min</th>\n"
    +     "<th>5min</th>\n"
    +     "<th>15min</th>\n"
    +     "<th>RPCs</th>\n"    // Threads
    +     "<th>HI</th>\n"
    +     "<th>Norm</th>\n"
    + "</tr></thead>\n"
    + "<tbody>\n"
    + "%tableRow{"
    + "  <tr>"
    + "    <td><a href='Remote?Node=%$host'>%node</a></td>"
    + "    <td>%num_cpus</td>"
    + "    <td>%num_keys</td>"
    + "    <td>%val_size</td>"
    + "    <td>%free_mem</td>"
    + "    <td>%tot_mem</td>"
    + "    <td>%max_mem</td>"
    + "    <td>%free_disk</td>"
    + "    <td>%max_disk</td>"
    + "    <td>%cpu_util</td>"
    + "    <td>%cpu_load_1</td>"
    + "    <td>%cpu_load_5</td>"
    + "    <td>%cpu_load_15</td>"
    + "    <td>%rpcs</td>"
    + "    <td style='%queueStyleHi'>%fjthrds_hi / %fjqueue_hi</td>"
    + "    <td style='%queueStyleLo'>%fjthrds_lo / %fjqueue_lo</td>"
    + "    <td>%tcps_active</td>"
    + "  </tr>\n"
    + "}"
    + "</tbody>"
    + "</table>\n"
    + "<p>%voting  %locked\n"
    ;
}
