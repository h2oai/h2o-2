package water.zookeeper.nodes;

import com.google.gson.Gson;

public class AbstractPayload {
  public byte[] toPayload() {
    Gson gson = new Gson();
    String json = gson.toJson(this);
    byte[] barr = json.getBytes();
    return barr;
  }

  static public <T> T fromPayload(byte[] payload, Class<T> klass) {
    Gson gson = new Gson();
    String s = new String(payload);
    T obj = gson.fromJson(s, klass);
    return obj;
  }
}
