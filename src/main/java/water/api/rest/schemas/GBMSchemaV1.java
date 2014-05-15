package water.api.rest.schemas;

import water.Key;
import water.api.Direction;
import water.api.DocGen;
import water.api.anno.RESTCall;
import water.api.rest.*;
import water.api.rest.Version.V1;

/** Actual schema for GBM REST API call.
 *
 * Aspects to express:
 *  - default values
 *
 * NOTE: now extends Request2 since we have to have a nice way to test it. */
@RESTCall(location="/metadata/models/algos/gbm/schema", path="/models/", method="PUT") // /metadata/models/algos
public class GBMSchemaV1 extends ApiSupport<GBMSchemaV1, Version.V1> {

  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API( help      = "Source frame", // <-- Short help
        helpFiles = {"source.rst", "general.rst"}, // <-- Help files used for documentation generation
        filter    = Default.class, //<-- here i need to preserve still filter field since it enforce generation of right control element
        direction = Direction.IN,
        required  = true,
        path      = "/frames/$self" //<-- where to fetch this resource
        )
  public String source;

  @API( help="Response",
        filter=Default.class,
        direction=Direction.INOUT, required=true, dependsOn="source",
        values="/frames/${source}/cols") // <-- We need to express available values
  public String response;

  @API(help="Selected columns", filter=Default.class, direction=Direction.IN)
  public int[] cols;

  @API(help="Number of trees", filter=Default.class, direction=Direction.IN)
  int ntrees = 10;

  @API(help = "Learning rate, from 0. to 1.0", direction=Direction.IN)
  public double learn_rate = 0.1;

  @API(help = "Execute classification",
       visible="",
       enabled="",
       valid="/frames/${/parameters/source}/cols/${/parameters/response}/type != 'Float' && ${/parameters/learn_rate} > 1000")
  // @Tom proposal: valid="(getFrame(source).getCol(response).getType() != 'Float') && (getParameter(learn_rate) > 1000)")
  public boolean classification ;

  // Output
  @API(help = "Destination key of produced model.")
  public Key destination_key;
  @API(help = "Job key of model producer.")
  public Key job_key;

  @Override public V1 getVersion() {
    return Version.v1;
  }
}