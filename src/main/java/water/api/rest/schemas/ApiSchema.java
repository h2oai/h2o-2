package water.api.rest.schemas;

import water.Iced;
import water.api.rest.Version;

// TODO: currently extends Iced so we get JSON serialization / deserialization
public class ApiSchema<V extends Version> extends Iced {
  public Version getVersion() {
    throw new UnsupportedOperationException("Can't get version for class: " + this.getClass());
  }
}
