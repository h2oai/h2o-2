package water.api.rest.schemas;

import water.Key;
import water.api.Direction;
import water.api.DocGen;
import water.api.Request;
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
public class GBMSchemaV1 extends ApiSchema<Version.V1> implements REST.Versioned<V1> {

  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @Request.API( help      = "Source frame", // <-- Short help
        helpFiles = {"source.rst", "general.rst"}, // <-- Help files used for documentation generation
        filter    = Request.Default.class, //<-- here i need to preserve still filter field since it enforce generation of right control element
        direction = Direction.IN,
        required  = true,
        path      = "/frames/$self" //<-- where to fetch this resource
        )
  public String source;

  @Request.API( help="Response",
        filter=Request.Default.class,
        direction=Direction.INOUT, required=true, dependsOn="source",
        values="/frames/${source}/cols") // <-- We need to express available values
  public String response;

  @Request.API(help="Selected columns", filter=Request.Default.class, direction=Direction.IN)
  public int[] cols;

  @Request.API(help="Number of trees", filter=Request.Default.class, direction=Direction.IN)
  int ntrees = 10;

  @Request.API(help = "Learning rate, from 0. to 1.0", direction=Direction.IN)
  public double learn_rate = 0.1;

  @Request.API(help = "Execute classification",
       visible="",
       enabled="",
       valid="/frames/${/parameters/source}/cols/${/parameters/response}/type != 'Float' && ${/parameters/learn_rate} > 1000")
  // @Tom proposal: valid="(getFrame(source).getCol(response).getType() != 'Float') && (getParameter(learn_rate) > 1000)")
  public boolean classification ;

  // Output
  @Request.API(help = "Destination key of produced model.")
  public Key destination_key;
  @Request.API(help = "Job key of model producer.")
  public Key job_key;

  @Override public V1 getVersion() {
    return Version.v1;
  }
}