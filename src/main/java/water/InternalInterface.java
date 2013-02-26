package water;

import hex.DGLM.GLMModel;
import hex.KMeans.KMeansModel;
import hex.rf.RFModel;

import java.io.InputStream;

import water.api.Cloud;

import com.google.gson.JsonObject;

public class InternalInterface implements water.ExternalInterface {
  public Key makeKey( String key_name ) { return Key.make(key_name); }
  public Value makeValue( Object key, byte[] bits ) { return new Value((Key)key,bits); }
  public void put( Object key, Object val ) { UKV.put((Key)key,(Value)val); }
  public Value  getValue( Object key ) { return UKV.getValue((Key)key); }
  public byte[] getBytes( Object val ) { return ((Value)val).memOrLoad(); }

  public Model ingestRFModelFromR( Object key, InputStream is ) {
    return null;
  }

  // All-in-one call to lookup a model, map the columns and score
  public double scoreKey( Object modelKey, String [] colNames, double[] row ) {
    Key key = (Key)modelKey;
    String sk = key.toString();
    Value v = DKV.get(key);
    if (v == null)
      throw new IllegalArgumentException("Key "+sk+" not found!");
    Model M = null;
    try {
      // TODO - replace me with proper typed Values
      if( sk.startsWith(   GLMModel.KEY_PREFIX)  ) M = new    GLMModel();
      if( sk.startsWith(KMeansModel.KEY_PREFIX)  ) M = new KMeansModel();
      if( sk.startsWith(    RFModel.KEY_PREFIX)  ) M = new     RFModel();
      M.read(new AutoBuffer(v.memOrLoad()));
    } catch(Throwable t) {
      throw new IllegalArgumentException("Key "+sk+" is not a Model key");
    }
    return scoreModel(M,colNames,row);
  }

  // Call to map the columns and score
  public double scoreModel( Object model, String [] colNames, double[] row ) {
    Model M = (Model)model;
    int[] map = M.columnMapping( colNames);
    if( !Model.isCompatible(map) )
      throw new IllegalArgumentException("This model uses different columns than those provided");
    return M.score(row,map);
  }

  public JsonObject cloudStatus( ) { return new Cloud().serve().toJson(); }
}
