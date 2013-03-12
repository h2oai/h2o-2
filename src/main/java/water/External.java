package water;

import com.google.gson.JsonObject;
import java.io.InputStream;

/**
 *  Expose internal H2O API calls to the External World
 *
 *  H2O innards need to go through the H2O ClassLoader to get bytecode
 *  reweaving done - e.g. adding serialization methods, or auto-distribution
 *  code.  The outside world probably does not want to use the H2O ClassLoader
 *  so this class bridges the gap by delegating all calls through a singleton
 *  interface object loaded by H2O.
 */
public class External {
  public final static ExternalInterface API;
  static {
    ExternalInterface api = null;
    try { api = (ExternalInterface)Boot._init.loadClass("water.InternalInterface").newInstance(); }
    catch( ClassNotFoundException e ) { }
    catch( InstantiationException e ) { }
    catch( IllegalAccessException e ) { }
    API = api;
  }

  public static Object makeKey  ( String key_name )     { return API.makeKey(key_name); }
  public static Object makeValue(Object key,byte[] bits){ return API.makeValue(key,bits); }
  public static void   put( Object key, Object val)     {        API.put(key,val); }
  public static Object getValue( Object key )           { return API.getValue(key); }
  public static Object getBytes( Object val )           { return API.getBytes(val); }

  public static Object ingestRFModelFromR(Object key,InputStream is){ return API.ingestRFModelFromR(key,is); }
  public static double scoreKey  ( Object modelKey, String [] colNames, double[] row ) { return API.scoreKey  (modelKey,colNames,row); }
  public static double scoreModel( Object model   , String [] colNames, double[] row ) { return API.scoreModel(model   ,colNames,row); }

  public static JsonObject  cloudStatus( )                      { return API.cloudStatus(); }
}
