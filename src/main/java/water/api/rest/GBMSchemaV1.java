package water.api.rest;

import hex.gbm.GBM;
import water.Key;
import water.api.rest.REST.ApiAdaptor;
import water.api.rest.REST.ApiSupport;
import water.api.rest.REST.RestCall;
import water.api.rest.Version.V1;
import water.fvec.Frame;

/** Actual schema for GBM REST API call
 *
 * NOTE: now extends Request2 since we have to have a nice way to test it. */
public class GBMSchemaV1 extends ApiSupport implements RestCall<Version.V1> {

  @API(help="Source frame", helpFiles={"source.rst", "general.rst"}, filter=Default.class, required=true)
  public Frame source;

  @API(help="Response", filter=Default.class, required=true, json = true, dependsOn="source")
  public String response;

  @API(help="Selected columns", filter=Default.class)
  public int[] cols;

  @API(help="Number of trees", filter=Default.class)
  int ntrees = 10;

  @API(help = "Learning rate, from 0. to 1.0", filter=Default.class)
  public double learn_rate = 0.1;

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