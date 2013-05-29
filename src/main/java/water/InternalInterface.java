package water;

import java.io.InputStream;

import water.api.Cloud;
import water.util.Log;

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
    try {
      return scoreModel(v.get(),colNames,row);
    } catch(Throwable t) {
      Log.err(t);
      throw new IllegalArgumentException("Key "+sk+" is not a Model key");
    }
  }

  public Model adaptModel(Object model, String [] colNames){
    return ((Model)model).adapt(colNames);
  }
  // Call to map the columns and score
  public double scoreModel( Object model,double[] row ) {
    return ((Model)model).score(row);
  }

  public JsonObject cloudStatus( ) { return new Cloud().serve().toJson(); }

  @Override public double scoreModel(Object model, String[] colNames, double[] row) {
    return adaptModel(model, colNames).score(row);
  }

}
