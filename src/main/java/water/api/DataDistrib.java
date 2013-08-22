package water.api;

import java.util.ArrayList;
import java.util.Arrays;

import water.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class DataDistrib extends Request {
  protected final H2OHexKey _dataKey = new H2OHexKey(DATA_KEY);

  @Override protected Response serve() {
    int cloudSize = H2O.CLOUD.size();
    ValueArray data = _dataKey.value();
    long chunksCount = data.chunks();
    ArrayList<ChunkInfo> chunks = new ArrayList<DataDistrib.ChunkInfo>();
    for (long ci=0; ci<chunksCount; ci++) {
      // collect info about chunks
      Key cKey = data.getChunkKey(ci);
      int nodeIdx = cKey.home(H2O.CLOUD);
      int rows = data.rpc(ci);
      chunks.add(new ChunkInfo(cKey, rows, nodeIdx));
    }
    // Create JSON response
    JsonObject jresult = new JsonObject();
    jresult.addProperty(DATA_KEY, data._key.toString());
    jresult.addProperty(ROWS, data.numRows());
    // data distribution per node
    long[] rowsPerNode = new long[cloudSize];
    ArrayList<Key>[] keysPerNode = new ArrayList[cloudSize];
    for (int i=0; i<cloudSize; i++) keysPerNode[i] = new ArrayList<Key>();
    for (ChunkInfo ci : chunks) {
      rowsPerNode[ci._node] += ci._rows;
      keysPerNode[ci._node].add(ci._key);
    }

    JsonArray nodesInfo = new JsonArray();
    for (int i=0;i<cloudSize;i++) {
      JsonObject ni = new JsonObject();
      ni.addProperty(NODE, i);
      ni.addProperty(ROWS, rowsPerNode[i]);
      ni.addProperty(KEYS, Arrays.toString(keysPerNode[i].toArray()));
      nodesInfo.add(ni);
    }
    jresult.add(NODES, nodesInfo);

    return Response.done(jresult);
  }

  static class ChunkInfo {
    public ChunkInfo(Key key, int rows, int node) {
      super();
      _key = key;
      _rows = rows;
      _node = node;
    }
    Key _key;
    int _rows;
    int _node;
  }
}
