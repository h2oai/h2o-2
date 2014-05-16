package water.api.rest.schemas;

import water.Iced;
import water.api.rest.RestUtils;
import water.api.rest.Version;

// TODO: currently extends Iced so we get JSON serialization / deserialization
public abstract class ApiSchema<V extends Version> extends Iced {
  public V getVersion() {
    throw RestUtils.barf(this);
  }
}
