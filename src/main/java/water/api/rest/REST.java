package water.api.rest;

import java.util.HashMap;
import java.util.Map;

import water.Key;
import water.UKV;
import water.fvec.Frame;
import water.fvec.Vec;

public class REST {

  private static Map<Class, ApiAdaptor> API_MAPPING = new HashMap<Class, ApiAdaptor>();
  private static Map<Class, ApiAdaptor> REVERSE_API_MAPPING = new HashMap<Class, ApiAdaptor>();
  public static Map<TransfSig, ValueTransf> VAL_TRANSF = new HashMap<TransfSig, ValueTransf>();
  static {
    // Later we will auto-generate this by reflection or Weaver
    registerAdaptor(new GBMAdaptorV1());
    registerAdaptor(new GBMAdaptorBloody());
    registerAdaptor(new CloudStatusAdaptorV1());

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

