package water.api.rest;

import java.util.HashMap;
import java.util.Map;

import water.*;
import water.api.rest.schemas.GBMSchemaBloody;
import water.api.rest.schemas.GBMSchemaV1;
import water.fvec.Frame;
import water.fvec.Vec;

public class REST {

  public static Map<Class, ApiAdaptor> API_MAPPING = new HashMap<Class, ApiAdaptor>();
  public static Map<TransfSig, ValueTransf> VAL_TRANSF = new HashMap<TransfSig, ValueTransf>();
  static {
    // Just temporary implementation of mapping between Schema and Adaptor
    // Later we will auto-generate this by reflection or Weaver
    API_MAPPING.put(GBMSchemaV1.class, new GBMAdaptorV1());
    API_MAPPING.put(GBMSchemaBloody.class, new GBMAdaptorBloody());

    VAL_TRANSF.put(String2Frame.transf(), new String2Frame());
    VAL_TRANSF.put(Frame2String.transf(), new Frame2String());
    VAL_TRANSF.put(String2Vec.transf(), new String2Vec());
    VAL_TRANSF.put(Vec2String.transf(), new Vec2String());
    VAL_TRANSF.put(String2Key.transf(), new String2Key());
    VAL_TRANSF.put(Key2String.transf(), new Key2String());
  }

  /** Abstract representation of a REST API call */
  interface RestCall<T extends Version> {
    public T getVersion();
  }

  /**
   * @param <I> implementation type
   * @param <A> api type
   * @param <V> version type
   */
  interface ApiAdaptor<I extends Iced, A extends RestCall<? super V>, V extends Version> {
      // Make implementation object based on given api object
      public abstract I makeImpl(A api);
      // Make API object based on implementation object
      public abstract A makeApi(I impl);
      // Transfer inputs from API to implementation
      public abstract I fillImpl(A api, I impl);
      // Transfer outputs from implementation to API
      public abstract A fillApi (I impl, A api);
      // Get supported version
      public abstract V getVersion();
      // Just creates empty implementation object
      public abstract I createImpl();
      // Just creates empty API object
      public abstract A createApi();
  }

  public static abstract class AbstractApiAdaptor<I extends Iced, O extends RestCall<? super V>, V extends Version> implements ApiAdaptor<I,O,V> {
    @Override public I makeImpl(O api) {
      I impl = createImpl();
      return fillImpl(api, impl);
    }
    @Override public O makeApi(I impl) {
      O api = createApi();
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

