package water.api.rest;

import water.Key;
import water.api.anno.RESTCall;
import water.api.rest.REST.ApiSupport;
import water.api.rest.Version.V1;

/** Actual schema for GBM REST API call.
 *
 * Aspects to express:
 *  - default values
 *
 * NOTE: now extends Request2 since we have to have a nice way to test it. */
@RESTCall(location="/metadata/models/algos/gbm/schema", path="/models/", method="PUT") // /metadata/models/algos
public class GBMSchemaV1 extends ApiSupport<GBMSchemaV1, Version.V1> {

  @API( help="Source frame", helpFiles={"source.rst", "general.rst"},
        filter=Default.class, //<-- here i need to preserve still filter field since it enforce generation of right control element
        direction=Direction.IN, required=true, path="/frames/$self")
  public String source;

  @API( help="Response",
        filter=Default.class,
        direction=Direction.INOUT, required=true, dependsOn="source",
        values="/frames/${source}/cols?names") // <-- We need to express what are available values
  public String response;

  @API(help="Selected columns", filter=Default.class, direction=Direction.IN)
  public int[] cols;

  @API(help="Number of trees", filter=Default.class, direction=Direction.IN)
  int ntrees = 10;

  @API(help = "Learning rate, from 0. to 1.0", direction=Direction.IN)
  public double learn_rate = 0.1;

  @API(help = "Execute classification",
       valid="/frames/${/parameters/source}/cols/${/parameters/response}/type != 'Float' && ${/parameters/learn_rate} > 1000")
  // @Tom proposal: valid="(getFrame(source).getCol(response).getType() != 'Float') && (getParameter(learn_rate) > 1000)")
  public boolean classification ;

  // Output
  @API(help = "Destination key")
  public Key destination_key;
  @API(help = "Job key")
  public Key job_key;

  @Override public V1 getVersion() {
    return Version.v1;
  }
}