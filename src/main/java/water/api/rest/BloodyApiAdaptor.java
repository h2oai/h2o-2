package water.api.rest;

import water.Iced;
import water.api.rest.Version.Bloody;
import water.api.rest.schemas.ApiSchema;

/**
 *
 *
 * @param <I> implementation type
 * @param <A> api type
 */
abstract class BloodyApiAdaptor<I extends Iced, A extends ApiSchema<Bloody>> extends AbstractApiAdaptor<I, A, Version.Bloody> {
  @Override public A fillApi(I impl, A api) {
    RestUtils.fillAPIFields(impl, api, REST.VAL_TRANSF);
    return api;
  }
  @Override public I fillImpl(A api, I impl) {
    RestUtils.fillImplFields(api, impl, REST.VAL_TRANSF);
    return impl;
  }

  @Override public water.api.rest.Version.Bloody getVersion() { return Version.bloody; };
}
