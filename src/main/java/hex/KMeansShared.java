package hex;

import java.util.Random;

import water.Job.FrameJob;

public abstract class KMeansShared extends FrameJob {
  @API(help = "Minimum change in clusters before stopping. Can be used as alternative to max_iter.")
  double epsilon = 1e-4;

  @API(help = "Seed for the random number generator")
  long seed = new Random().nextLong();

  @API(help = "Whether data should be normalized")
  boolean normalize;
}
