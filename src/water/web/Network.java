package water.web;

import java.util.Properties;

import com.google.gson.JsonObject;

import water.*;

/**
 * Network statistics web page.
 *
 * The page contains network statistics for each running node (JVM).
 *
 * @author Michal Malohlava
 */
public class Network extends H2OPage {

  public Network() {
    _refresh = 5;
  }

  @Override
  public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    JsonObject res = new JsonObject();
    final H2O cloud = H2O.CLOUD;
    res.addProperty("cloud_name", H2O.NAME);
    res.addProperty("node_name", H2O.SELF.toString());
    res.addProperty("cloud_size",cloud._memary.length);

    // Returns only this node statistics
    HeartBeat hb = H2O.SELF._heartbeat;
    res.addProperty("total_in_conn",  toPosNumber(hb._total_in_conn));
    res.addProperty("total_out_conn", toPosNumber(hb._total_out_conn));
    res.addProperty("tcp_in_conn",    toPosNumber(hb._tcp_in_conn));
    res.addProperty("tcp_out_conn",   toPosNumber(hb._tcp_out_conn));
    res.addProperty("udp_in_conn",    toPosNumber(hb._udp_in_conn));
    res.addProperty("udp_out_conn",   toPosNumber(hb._udp_out_conn));

    // fill total traffic statistics
    res.addProperty("total_packets_recv",    toPosNumber               (hb._total_packets_recv));
    res.addProperty("total_bytes_recv",      PrettyPrint.bytes         (hb._total_bytes_recv));
    res.addProperty("total_bytes_recv_rate", PrettyPrint.bytesPerSecond(hb._total_bytes_recv_rate));
    res.addProperty("total_packets_sent",    toPosNumber               (hb._total_packets_sent));
    res.addProperty("total_bytes_sent",      PrettyPrint.bytes         (hb._total_bytes_sent));
    res.addProperty("total_bytes_sent_rate", PrettyPrint.bytesPerSecond(hb._total_bytes_sent_rate));

    // fill TCP traffic statistics
    res.addProperty("tcp_packets_recv", toPosNumber      (hb._tcp_packets_recv));
    res.addProperty("tcp_bytes_recv",   PrettyPrint.bytes(hb._tcp_bytes_recv));
    res.addProperty("tcp_packets_sent", toPosNumber      (hb._tcp_packets_sent));
    res.addProperty("tcp_bytes_sent",   PrettyPrint.bytes(hb._tcp_bytes_sent));

    // fill UDP traffic statistics
    res.addProperty("udp_packets_recv", toPosNumber      (hb._udp_packets_recv));
    res.addProperty("udp_bytes_recv",   PrettyPrint.bytes(hb._udp_bytes_recv));
    res.addProperty("udp_packets_sent", toPosNumber      (hb._udp_packets_sent));
    res.addProperty("udp_bytes_sent",   PrettyPrint.bytes(hb._udp_bytes_sent));

    return res;
  }

  @Override
  protected String serveImpl(Server server, Properties args, String sessionID) {
    RString response = new RString(HTML_TEMPLATE);

    response.replace("cloud_name", H2O.NAME);
    response.replace("node_name", H2O.SELF.toString());

    final H2O cloud = H2O.CLOUD;
    for (H2ONode h2o : cloud._memary) {
      HeartBeat hb = h2o._heartbeat;
      RString row = response.restartGroup("tableRow");
      row.replace("node", h2o);
      // fill number of connections
      row.replace("total_in_conn", toPosNumber(hb._total_in_conn));
      row.replace("total_out_conn", toPosNumber(hb._total_out_conn));
      row.replace("tcp_in_conn", toPosNumber(hb._tcp_in_conn));
      row.replace("tcp_out_conn", toPosNumber(hb._tcp_out_conn));
      row.replace("udp_in_conn", toPosNumber(hb._udp_in_conn));
      row.replace("udp_out_conn", toPosNumber(hb._udp_out_conn));

      // fill total traffic statistics
      row.replace("total_packets_recv", toPosNumber(hb._total_packets_recv));
      row.replace("total_bytes_recv", PrettyPrint.bytes(hb._total_bytes_recv));
      row.replace("total_bytes_recv_rate", PrettyPrint.bytesPerSecond(hb._total_bytes_recv_rate));
      row.replace("total_packets_sent", toPosNumber(hb._total_packets_sent));
      row.replace("total_bytes_sent", PrettyPrint.bytes(hb._total_bytes_sent));
      row.replace("total_bytes_sent_rate", PrettyPrint.bytesPerSecond(hb._total_bytes_sent_rate));

      // fill TCP traffic statistics
      row.replace("tcp_packets_recv", toPosNumber(hb._tcp_packets_recv));
      row.replace("tcp_bytes_recv", PrettyPrint.bytes(hb._tcp_bytes_recv));
      row.replace("tcp_packets_sent", toPosNumber(hb._tcp_packets_sent));
      row.replace("tcp_bytes_sent", PrettyPrint.bytes(hb._tcp_bytes_sent));

      // fill UDP traffic statistics
      row.replace("udp_packets_recv", toPosNumber(hb._udp_packets_recv));
      row.replace("udp_bytes_recv", PrettyPrint.bytes(hb._udp_bytes_recv));
      row.replace("udp_packets_sent", toPosNumber(hb._udp_packets_sent));
      row.replace("udp_bytes_sent", PrettyPrint.bytes(hb._udp_bytes_sent));

      row.append();
    }

    return response.toString();
  }

  protected String toPosNumber(final long num) {
    if (num < 0) return "n/a";
    return String.valueOf(num);
  }

  // Note: Open Sockets | TCP/UDP | state | recvs, packets, read/s write/s bytes/s | sends, packets, read/s, write/s, bytes/s

  private static final String HTML_TEMPLATE = "<div class='alert alert-success'>"
    + "You are connected to cloud <strong>%cloud_name</strong> and node <strong>%node_name</strong>."
    + "</div>"
    + "<table class='table table-striped table-bordered table-condensed'>"
    + "<thead class=''>"
    + "<tr><th>&nbsp;</th><th colspan='3'>Connections</th><th colspan='3'>Network interface traffic</th><th colspan='2'>TCP traffic</th><th colspan='2'>UDP traffic</th></tr>"
    + "<tr><th>Nodes</th><th>Total IN / OUT</th><th>TCP IN / OUT</th><th>UDP IN / OUT</th><th>Packets IN / OUT</th><th>Bytes IN / OUT</th><th>Rate IN / OUT</th><th>Packets IN / OUT</th><th>Bytes IN / OUT</th><th>Packets IN / OUT</th><th>Bytes IN / OUT</th></tr>"
    + "</thead>"
    + "<tbody>"
    + "%tableRow{"
    + "  <tr>"
    + "    <td>%node</td>"
    + "    <td>%total_in_conn / %total_out_conn</td>"
    + "    <td>%tcp_in_conn / %tcp_out_conn</td>"
    + "    <td>%udp_in_conn / %udp_out_conn</td>"
    + "    <td>%total_packets_recv / %total_packets_sent</td>"
    + "    <td>%total_bytes_recv / %total_bytes_sent</td>"
    + "    <td>%total_bytes_recv_rate / %total_bytes_sent_rate</td>"
    + "    <td>%tcp_packets_recv / %tcp_packets_sent</td>"
    + "    <td>%tcp_bytes_recv / %tcp_bytes_sent</td>"
    + "    <td>%udp_packets_recv / %udp_packets_sent</td>"
    + "    <td>%udp_bytes_recv / %udp_bytes_sent</td>"
    + "  </tr>"
    + "}"
    + "</tbody>"
    + "</table>\n";
}
