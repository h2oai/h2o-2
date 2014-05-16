package water.api.rest;

import java.util.HashMap;
import java.util.Map;

import water.*;
import water.api.rest.handlers.ModelHandlerV3;
import water.api.rest.schemas.ApiSchema;
import water.fvec.Frame;
import water.api.rest.schemas.GBMSchemaBloody;
import water.api.rest.schemas.GBMSchemaV1;
import water.fvec.Vec;
import water.util.Log;

public class REST {

  private static Map<Class, ApiAdaptor> API_MAPPING = new HashMap<Class, ApiAdaptor>();
  private static Map<Class, ApiAdaptor> REVERSE_API_MAPPING = new HashMap<Class, ApiAdaptor>();
  public static Map<TransfSig, ValueTransf> VAL_TRANSF = new HashMap<TransfSig, ValueTransf>();
  static {
    // Later we will auto-generate this by reflection or Weaver
    registerAdaptor(new GBMAdaptorV1());
    registerAdaptor(new GBMAdaptorBloody());

    VAL_TRANSF.put(String2Frame.transf(), new String2Frame());
    VAL_TRANSF.put(Frame2String.transf(), new Frame2String());
    VAL_TRANSF.put(String2Vec.transf(), new String2Vec());
    VAL_TRANSF.put(Vec2String.transf(), new Vec2String());
    VAL_TRANSF.put(String2Key.transf(), new String2Key());
    VAL_TRANSF.put(Key2String.transf(), new Key2String());
  }

  static public ApiAdaptor registerAdaptor(ApiAdaptor adaptor) {
    Class apiClass = adaptor.getApiClass();
    Class implClass = adaptor.getImplClass();
    ApiAdaptor old = API_MAPPING.get(apiClass);

    API_MAPPING.put(apiClass, adaptor);
    REVERSE_API_MAPPING.put(implClass, adaptor);
    return old;
  }

  static public ApiAdaptor unregisterAdaptor(Class schema) {
    ApiAdaptor old = API_MAPPING.get(schema);
    API_MAPPING.remove(schema);
    return old;
  }

  static public ApiAdaptor getAdaptorFromSchema(Class schema) {
    return API_MAPPING.get(schema);
  }

  static public ApiAdaptor getAdaptorFromImpl(Class impl) {
    return REVERSE_API_MAPPING.get(impl);
  }

  /** Abstract representation of a REST API call */
  public interface Versioned<T extends Version> {
    public T getVersion();
  }

  /**
   * @param <I> implementation type
   * @param <A> api type
   * @param <V> version type
   */
  public interface ApiAdaptor<I extends Iced, A extends ApiSchema<? super V>, V extends Version> {
    // Make implementation object based on given api object
    public I createAndAdaptToImpl(A api);
    // Make API object based on implementation object
    public A createAndAdaptToApi(I impl);
    // Transfer inputs from API to implementation
    public I fillImpl(A api, I impl);
    // Transfer outputs from implementation to API
    public A fillApi (I impl, A api);
    // Get supported version
    public V getVersion();
    // Just creates empty implementation object
    public I createImpl();
    // Just creates empty API object
    public A createApi();
    // Get the class for the implementation object
    public Class<I> getImplClass();
    // Get the class for the api object
    public Class<A> getApiClass();
  }

  public static abstract class AbstractApiAdaptor<I extends Iced, A extends ApiSchema<? super V>, V extends Version> implements ApiAdaptor<I,A,V> {
    @Override public A createApi() {
      A api = null;
      try {
        api = getApiClass().newInstance();
      }
      catch (Exception e) {
        Log.warn("Caught exception trying to create a: " + getApiClass().toString());
      }
      return api;
    }

    @Override public I createImpl() {
      I impl = null;
      try {
        impl = getImplClass().newInstance();
      }
      catch (Exception e) {
        Log.warn("Caught exception trying to create a: " + getImplClass().toString());
      }
      return impl;
    }

    @Override public I createAndAdaptToImpl(A api) {
      I impl = createImpl();
      return fillImpl(api, impl);
    }
    @Override public A createAndAdaptToApi(I impl) {
      A api = createApi();
      return fillApi(impl, api);
    }
  }

  public static final class TransfSig extends Tuple<Class, Class> {
    public TransfSig(Class f, Class l) { super(f,l); }
  }
  // Tranformation from type T1 to T2
  public interface ValueTransf<T1, T2> {
    public T2 from(T1 t);
  }

  // Ahhhh - Java is evil, missing type tags causing so noisy code :-(
  static class String2Frame implements ValueTransf<String, Frame> {
    @Override public Frame from(String t) { return UKV.get(Key.make(t)); }
    public static TransfSig transf() { return RestUtils.tsig(String.class, Frame.class); }
  }
  static class Frame2String implements ValueTransf<Frame, String> {
    @Override public String from(Frame t) { return t._key.toString(); }
    public static TransfSig transf() { return RestUtils.tsig(Frame.class, String.class); }
  }
  static class String2Vec implements ValueTransf<String, Vec> {
    @Override public Vec from(String t) { return UKV.get(Key.make(t)); }
    public static TransfSig transf() { return RestUtils.tsig(String.class, Vec.class); }
  }
  static class Vec2String implements ValueTransf<Vec, String> {
    @Override public String from(Vec t) { return t._key.toString(); }
    public static TransfSig transf() { return RestUtils.tsig(Vec.class, String.class); }
  }
  static class String2Key implements ValueTransf<String, Key> {
    @Override public Key from(String t) { return Key.make(t); }
    public static TransfSig transf() { return RestUtils.tsig(String.class, Key.class); }
  }
  static class Key2String implements ValueTransf<Key, String> {
    @Override public String from(Key t) { return t.toString(); }
    public static TransfSig transf() { return RestUtils.tsig(Key.class, String.class); }
  }
}

