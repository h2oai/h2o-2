package hex;

import java.util.Random;

import water.Job.HexJob;
import water.api.DocGen;

public abstract class KMeansShared extends HexJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Minimum change in clusters before stopping", filter = Default.class)
  double epsilon = 1e-4;

  @API(help = "Seed for the random number generator", filter = Default.class)
  long seed = new Random().nextLong();

  @API(help = "Whether data should be normalized", filter = Default.class)
  boolean normalize;
}
