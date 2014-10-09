package water;

import dontweave.gson.JsonObject;
import java.io.InputStream;

/**
 *  External interface for H2O.
 *
 *  All functions are delegated through an InternalInterface object 
 *  which will use the H2O.Boot class loader.
 */

public interface ExternalInterface {
  public Object makeKey  ( String key_name );
  public Object makeValue( Object key, byte[] bits );
  public void   put      ( Object key, Object val );
  public Object getValue ( Object key );
  public byte[] getBytes ( Object val );

  public Object ingestRFModelFromR(Object key, InputStream is);
  public float[] scoreKey  ( Object modelKey, String [] colNames, String [][] domains, double[] row );
  public float[] scoreModel( Object model   , String [] colNames, String [][] domains, double[] row );

  public JsonObject  cloudStatus( );
}
