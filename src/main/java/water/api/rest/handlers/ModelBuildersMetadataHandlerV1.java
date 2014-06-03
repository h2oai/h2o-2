package water.api.rest.handlers;

import water.H2O;
import water.api.rest.Version;
import water.schemas.ModelBuildersMetadataV1;

import java.util.ArrayList;
import java.util.List;

public class ModelBuildersMetadataHandlerV1<V extends Version> extends AbstractHandler<V> {
  public ModelBuildersMetadataHandlerV1(String path) {
    super(path);
  }

}
