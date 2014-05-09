package water.api.rest;

import hex.gbm.GBM;
import water.Key;
import water.Request2;
import water.api.rest.REST.ApiAdaptor;
import water.api.rest.REST.RestCall;
import water.api.rest.Version.V1;
import water.fvec.Frame;

/** Actual schema for GBM REST API call
 *
 * NOTE: now extends Request2 since we have to have a nice way to test it. */
public class GBMAPI extends Request2 implements RestCall<Version.V1> {

  @API(help="Source frame", filter=Default.class, required=true)
  Frame source;
  @API(help="Selected columns", filter=Default.class)
  public int[] cols;
  @API(help="Number of trees", filter=Default.class)
  int ntrees;
  @API(help = "Learning rate, from 0. to 1.0", filter=Default.class)
  public double learn_rate = 0.1;
  // Output
  @API(help = "Destination key")
  public Key destination_key;
  @API(help = "Job key")
  public Key job_key;

  @Override protected Response serve() {
    ApiAdaptor adaptor = REST.API_MAPPING.get(this.getClass());
    GBM gbm = (GBM) adaptor.makeImpl(this);
    Response r = gbm.servePublic();
    adaptor.fillApi(gbm, this);
    return r;
  }
  @Override public V1 getVersion() {
    return Version.v1;
  }
}