
package water.api;

import water.*;

import com.google.gson.*;

public class Network extends Request {

  public Network() {
    _requestHelp = "Displays network statistics for each node in the cloud.";
  }

  @Override public Response serve() {
    JsonObject json = new JsonObject();
    final H2O cloud = H2O.CLOUD;
    final H2ONode self = H2O.SELF;
    json.addProperty(CLOUD_NAME, H2O.NAME);
    json.addProperty(NODE_NAME, self.toString());

    JsonArray nodes = new JsonArray();
    for (H2ONode h2o : cloud._memary) {
      JsonObject n = new JsonObject();
      HeartBeat hb = h2o._heartbeat;
      n.addProperty(NODE, h2o.toString());
      // fill number of connections
      n.addProperty(TOTAL_CONN_IN, hb._total_in_conn);
      n.addProperty(TOTAL_CONN_OUT, hb._total_out_conn);
      n.addProperty(TCP_CONN_IN, hb._tcp_in_conn);
      n.addProperty(TCP_CONN_OUT, hb._tcp_out_conn);
      n.addProperty(UDP_CONN_IN, hb._udp_in_conn);
      n.addProperty(UDP_CONN_OUT, hb._udp_out_conn);

      // fill total traffic statistics
      n.addProperty(TOTAL_PACKETS_RECV, hb._total_packets_recv);
      n.addProperty(TOTAL_BYTES_RECV, hb._total_bytes_recv);
      n.addProperty(TOTAL_BYTES_RECV_RATE, hb._total_bytes_recv_rate);
      n.addProperty(TOTAL_PACKETS_SENT, hb._total_packets_sent);
      n.addProperty(TOTAL_BYTES_SENT, hb._total_bytes_sent);
      n.addProperty(TOTAL_BYTES_SENT_RATE, hb._total_bytes_sent_rate);

      // fill TCP traffic statistics
      n.addProperty(TCP_PACKETS_RECV, hb._tcp_packets_recv);
      n.addProperty(TCP_BYTES_RECV, hb._tcp_bytes_recv);
      n.addProperty(TCP_PACKETS_SENT, hb._tcp_packets_sent);
      n.addProperty(TCP_BYTES_SENT, hb._tcp_bytes_sent);

      // fill UDP traffic statistics
      n.addProperty(UDP_PACKETS_RECV, hb._udp_packets_recv);
      n.addProperty(UDP_BYTES_RECV, hb._udp_bytes_recv);
      n.addProperty(UDP_PACKETS_SENT, hb._udp_packets_sent);
      n.addProperty(UDP_BYTES_SENT, hb._udp_bytes_sent);

      nodes.add(n);
    }

    json.add(NODES, nodes);
    Response r = Response.done(json);
    r.setBuilder(NODES, new NodeTableBuilder());
    return r;
  }
  private static class NodeTableBuilder extends ArrayBuilder {
    @Override
    public String header(JsonArray array) {
      return "<table class='table table-striped table-bordered table-condensed'>" +
          "<thead>" +
          "<tr>" +
          "  <th></th>" +
          "  <th colspan='3'>Connections</th>" +
          "  <th colspan='3'>Network interface traffic</th>" +
          "  <th colspan='2'>TCP traffic</th>" +
          "  <th colspan='2'>UDP traffic</th>" +
          "</tr><tr>" +
          "  <th>Nodes</th>" +
          "  <th>Total IN / OUT</th>" +
          "  <th>TCP IN / OUT</th>" +
          "  <th>UDP IN / OUT</th>" +
          "  <th>Packets IN / OUT</th>" +
          "  <th>Bytes IN / OUT</th>" +
          "  <th>Rate IN / OUT</th>" +
          "  <th>Packets IN / OUT</th>" +
          "  <th>Bytes IN / OUT</th>" +
          "  <th>Packets IN / OUT</th>" +
          "  <th>Bytes IN / OUT</th>" +
          "</tr>" +
          "</thead><tbody>";
    }

    @Override
    public String footer(JsonArray array) {
      return "</tbody></table>";
    }

    @Override
    public Builder defaultBuilder(JsonElement element) {
      return new NodeRowBuilder();
    }
}
private static class NodeRowBuilder extends ArrayRowBuilder {
  private void td(StringBuilder sb, JsonObject json, String... props) {
    sb.append("<td>");
    String sep = "";
    for( String p : props ) {
      sb.append(sep);
      sb.append(ELEMENT_BUILDER.elementToString(json.get(p), p));
      sep = " / ";
    }
    sb.append("</td>");
  }

  @Override
  public String build(Response response, JsonObject json, String contextName) {
    String name = elementName(contextName);
    StringBuilder sb = new StringBuilder();
    sb.append(caption(json, name));
    sb.append(header(json, name));
    td(sb, json, NODE);
    td(sb, json, TOTAL_CONN_IN, Constants.TOTAL_CONN_OUT);
    td(sb, json, TCP_CONN_IN, Constants.TCP_CONN_OUT);
    td(sb, json, UDP_CONN_IN, Constants.UDP_CONN_OUT);
    td(sb, json, TOTAL_PACKETS_RECV, Constants.TOTAL_PACKETS_SENT);
    td(sb, json, TOTAL_BYTES_SENT, Constants.TOTAL_BYTES_RECV);
    td(sb, json, TOTAL_BYTES_SENT_RATE, Constants.TOTAL_BYTES_RECV_RATE);
    td(sb, json, TCP_PACKETS_RECV, Constants.TCP_PACKETS_SENT);
    td(sb, json, TCP_BYTES_SENT, Constants.TCP_BYTES_RECV);
    td(sb, json, UDP_PACKETS_RECV, Constants.UDP_PACKETS_SENT);
    td(sb, json, UDP_BYTES_SENT, Constants.UDP_BYTES_RECV);
    sb.append(footer(json, name));
    return sb.toString();
  }
}
}
