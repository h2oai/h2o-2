package water;

import java.io.InputStream;

import water.util.Log;

import dontweave.gson.JsonObject;

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
    catch( ClassNotFoundException e ) { Log.err(e); }
    catch( InstantiationException e ) { Log.err(e); }
    catch( IllegalAccessException e ) { Log.err(e); }
    API = api;
  }

  public static Object makeKey  ( String key_name )     { return API.makeKey(key_name); }
  public static Object makeValue(Object key,byte[] bits){ return API.makeValue(key,bits); }
  public static void   put( Object key, Object val)     {        API.put(key,val); }
  public static Object getValue( Object key )           { return API.getValue(key); }
  public static Object getBytes( Object val )           { return API.getBytes(val); }

  public static Object ingestRFModelFromR(Object key,InputStream is){ return API.ingestRFModelFromR(key,is); }
  public static float[] scoreKey  ( Object modelKey, String [] colNames, String domains[][], double[] row ) { return API.scoreKey  (modelKey,colNames,domains,row); }
  public static float[] scoreModel( Object model   , String [] colNames, String domains[][], double[] row ) { return API.scoreModel(model   ,colNames,domains,row); }

  public static JsonObject  cloudStatus( )                      { return API.cloudStatus(); }
}
