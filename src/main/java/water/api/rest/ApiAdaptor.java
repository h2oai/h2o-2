package water.api.rest;

import water.Iced;
import water.api.rest.schemas.ApiSchema;

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