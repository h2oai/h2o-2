package water.api.rest.handlers;

import water.api.rest.REST;
import water.api.rest.Version;

public class ModelHandlerV3 extends ModelHandler<Version.V3> implements REST.Versioned<Version.V3> {
  public ModelHandlerV3(String path) {
        super(path);
    }

  @Override
    public Version.V3 getVersion() {
    return Version.v3;
  }
}
