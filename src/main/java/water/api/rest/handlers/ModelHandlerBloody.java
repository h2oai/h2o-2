package water.api.rest.handlers;

import water.api.rest.REST;
import water.api.rest.Version;

public class ModelHandlerBloody extends ModelHandler<Version.Bloody> {

  public ModelHandlerBloody(String path) {
    super(path);
  }

  @Override
    public Version.Bloody getVersion() {
    return Version.bloody;
  }
}
