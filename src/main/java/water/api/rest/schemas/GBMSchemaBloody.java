package water.api.rest.schemas;

import water.Key;
import water.api.Request;
import water.api.rest.*;
import water.api.rest.Version.Bloody;
import water.fvec.Frame;

/** Actual schema for GBM REST API call
 *
 * NOTE: now extends Request2 since we have to have a nice way to test it. */
public class GBMSchemaBloody extends ApiSchema<Bloody> {

  @Request.API(help="Source frame", helpFiles={"source.rst", "general.rst"}, filter=Request.Default.class, required=true)
  public Frame source;

  @Request.API(help="Response", filter=Request.Default.class, required=true, json = true, dependsOn="source")
  public String response;

  @Request.API(help="Selected columns", filter=Request.Default.class)
  public int[] cols;

  @Request.API(help="Number of trees", filter=Request.Default.class)
  int ntrees = 10;

  @Request.API(help = "Learning rate, from 0. to 1.0", filter=Request.Default.class)
  public double learn_rate = 0.1;

  // Output
  @Request.API(help = "Destination key")
  public Key destination_key;
  @Request.API(help = "Job key")
  public Key job_key;

  @Override public Bloody getVersion() {
    return Version.bloody;
  }
}
