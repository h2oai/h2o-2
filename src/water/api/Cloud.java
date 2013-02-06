package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import water.*;

public class Cloud extends Request {

  public Cloud() {
    _requestHelp = "Displays the information about the current cloud. For each"
            + " node displays its heartbeat information.";
  }

  @Override public Response serve() {
    JsonObject response = new JsonObject();
    final H2O cloud = H2O.CLOUD;
    final H2ONode self = H2O.SELF;
    response.addProperty(CLOUD_NAME, H2O.NAME);
    response.addProperty(NODE_NAME, self.toString());
    response.addProperty(CLOUD_SIZE, cloud._memary.length);
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
      node.addProperty(FREE_DISK, hb.get_free_disk());
      node.addProperty(MAX_DISK, hb.get_max_disk());
      node.addProperty(NUM_CPUS, (int)hb._num_cpus);
      node.addProperty(CPU_UTIL, hb.get_cpu_util());
      node.addProperty(CPU_LOAD_1, pos_neg(hb.get_cpu_load1()));
      node.addProperty(CPU_LOAD_5, pos_neg(hb.get_cpu_load5()));
      node.addProperty(CPU_LOAD_15, pos_neg(hb.get_cpu_load15()));
      node.addProperty(FJ_THREADS_HI, (int)hb._fjthrds_hi);
      node.addProperty(FJ_QUEUE_HI, (int)hb._fjqueue_hi);
      node.addProperty(FJ_THREADS_LO, (int)hb._fjthrds_lo);
      node.addProperty(FJ_QUEUE_LO, (int)hb._fjqueue_lo);
      node.addProperty(RPCS, (int)hb._rpcs);
      node.addProperty(TCPS_ACTIVE, (int) hb._tcps_active);
      nodes.add(node);
    }
    response.add(NODES,nodes);
    response.addProperty(CONSENSUS, Paxos._commonKnowledge); // Cloud is globally accepted
    response.addProperty(LOCKED, Paxos._cloudLocked); // Cloud is locked against changes
    Response r = Response.done(response);
    r.setBuilder(CONSENSUS, new BooleanStringBuilder("","Voting new members"));
    r.setBuilder(LOCKED, new BooleanStringBuilder("Locked","Accepting new members"));
    r.setBuilder(NODES+"."+NAME, new NodeCellBuilder());
    return r;
  }

  public static String pos_neg(double d) {
    return d >= 0 ? String.valueOf(d) : "n/a";
  }

  // Just the Node as a link
  public class NodeCellBuilder extends ArrayRowElementBuilder {
    @Override public String elementToString(JsonElement element, String contextName) {
      String str = element.getAsString();
      if( str.equals(H2O.SELF.toString()) )
        return "<a href='StoreView.html'>"+str+"</a>";
      return "<a href='Remote.html?Node="+str+"'>"+str+"</a>";
    }
  }
}
