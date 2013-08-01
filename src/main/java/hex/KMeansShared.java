package hex;

import java.util.Random;

import water.Job.FrameJob;

public class KMeansShared extends FrameJob {
  @API(help = "Minimum change in clusters before stopping. Can be used instead of, or in addition to max_iter")
  @Input
  @Bounds(min = 0, max = 1)
  double epsilon = 1e-4;

  @API(help = "Seed for the random number generator")
  @Input
  long seed = new Random().nextLong();

  @API(help = "Whether data should be normalized")
  @Input
  boolean normalize;
}
