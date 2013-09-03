package hex;

import hex.KMeans.Initialization;

import java.util.Random;

import water.Job.HexJob;
import water.api.DocGen;

public abstract class KMeansShared extends HexJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Clusters initialization", filter = Default.class)
  Initialization initialization = Initialization.None;

  @API(help = "Seed for the random number generator", filter = Default.class)
  long seed = new Random().nextLong();

  @API(help = "Whether data should be normalized", filter = Default.class)
  boolean normalize;
}
