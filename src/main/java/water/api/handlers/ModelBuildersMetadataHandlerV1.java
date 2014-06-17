package water.api.handlers;

import water.H2O;
import water.api.Handler;
import water.schemas.ModelBuildersMetadataV1;

import java.util.ArrayList;
import java.util.List;

public class ModelBuildersMetadataHandlerV1 extends Handler<ModelBuildersMetadataHandlerV1, ModelBuildersMetadataV1> {

  public List<ModelBuildersMetadataV1> list() {
    return new ArrayList<ModelBuildersMetadataV1>();
  }

  @Override public void compute2() {
    // Weh?!?
  }

  public ModelBuildersMetadataV1 show() {
    return new ModelBuildersMetadataV1();
  }

  @Override protected ModelBuildersMetadataV1 schema(int version) {
    switch (version) {
    case 1:
      return new ModelBuildersMetadataV1();
    default:
      throw H2O.fail("Unknown schema version: " + version + " for handler class: " + this.getClass());
    }
  }
}
