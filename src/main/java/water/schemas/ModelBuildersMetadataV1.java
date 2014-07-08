package water.schemas;

import water.api.handlers.ModelBuildersMetadataHandlerV1;

public class ModelBuildersMetadataV1 extends Schema<ModelBuildersMetadataHandlerV1, ModelBuildersMetadataV1> {

/*
  // Output fields
  @API(help="List of model builders.")
    List<ModelBuilder> modelBuilders;
*/

  // Version&Schema-specific filling into the handler
  public ModelBuildersMetadataV1 fillInto( ModelBuildersMetadataHandlerV1 h ) {
      throw new UnsupportedOperationException("ModelBuildersMetadataV1.fillInto");
  }

  // Version&Schema-specific filling from the handler
  public ModelBuildersMetadataV1 fillFrom( ModelBuildersMetadataHandlerV1 h ) {
      throw new UnsupportedOperationException("ModelBuildersMetadataV1.fillFrom");
  }

}
