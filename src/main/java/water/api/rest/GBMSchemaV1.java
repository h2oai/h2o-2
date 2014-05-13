package water.api.rest;

import hex.gbm.GBM;
import water.Key;
import water.api.rest.REST.ApiAdaptor;
import water.api.rest.REST.ApiSupport;
import water.api.rest.REST.RestCall;
import water.api.rest.Version.V1;
import water.fvec.Frame;
import water.fvec.Vec;

/** Actual schema for GBM REST API call
 *
 * NOTE: now extends Request2 since we have to have a nice way to test it. */
@REST(location="/metadata/models/algos/gbm/schema", href="/models/") // /metadata/models/algos
public class GBMSchemaV1 extends ApiSupport implements RestCall<Version.V1> {

  @API( help="Source frame", helpFiles={"source.rst", "general.rst"}, filter=Default.class, //<-- here i need to preserve still filter field since it enforce generation of right control element
        direction=Direction.IN, required=true, type=Frame.class, href="/frames/$self")
  public String source;

  @API( help="Response", filter=Default.class,
        direction=Direction.IN, required=true, json = true, dependsOn="source", type=Vec.class,
        values="/frames/${source}/cols?names")
  public String response;

  @API(help="Selected columns", filter=Default.class, direction=Direction.IN)
  public int[] cols;

  @API(help="Number of trees", filter=Default.class, direction=Direction.IN)
  int ntrees = 10;

  @API(help = "Learning rate, from 0. to 1.0", direction=Direction.IN)
  public double learn_rate = 0.1;

  @API(help = "Execute classification", valid="/frames/${/parameters/source}/cols/${/parameters/response}/type != 'Float' && ${/parameters/learn_rate} > 1000")
  public boolean classification ;

  // Output
  @API(help = "Destination key")
  public Key destination_key;
  @API(help = "Job key")
  public Key job_key;

  // This is a part of HACK to be connect a new API to current RequestServer
  @Override protected Response serve() {
    // Get adaptor
    ApiAdaptor adaptor = REST.API_MAPPING.get(this.getClass());
    // Create implementation
    GBM gbm = (GBM) adaptor.makeImpl(this);
    Response r = gbm.servePublic();
    // Fill API
    adaptor.fillApi(gbm, this);
    return r;
  }
  @Override public V1 getVersion() {
    return Version.v1;
  }
}