package water.api;

import java.util.Random;

import water.Key;
import water.ValueArray;

public abstract class KMeansShared extends Request {
  @API(help = "Key for input dataset")
  @Input
  final H2OHexKey source_key = new H2OHexKey("source_key");

  @API(help = "Minimum change in clusters before stopping. Can be used instead of, or in addition to max_iter")
  @Input
  @Bounds(min = 0, max = 1)
  final double epsilon = 1e-4;

  @API(help = "Seed for the random number generator")
  final LongInt seed = new LongInt("seed", new Random().nextLong(), "");

  @API(help = "Whether data should be normalized")
  final Bool normalize = new Bool("normalize", false, "");

  @API(help = "Columns to use as input")
  final HexAllColumnSelect cols = new HexAllColumnSelect("cols", source_key);

  @API(help = "Destination key")
  final H2OKey destination_key = new H2OKey("destination_key", hex.KMeans.makeKey());

  final hex.KMeans start(Key dest, int k, int maxIter) {
    ValueArray va = source_key.value();
    Key source = va._key;
    double ep = epsilon.value();
    long seed_ = seed.record()._valid ? seed.value() : seed._defaultValue;
    boolean norm = normalize.record()._valid ? normalize.value() : normalize._defaultValue;
    int[] columns = cols.value();

    if( dest == null ) {
      String n = source.toString();
      int dot = n.lastIndexOf('.');
      if( dot > 0 )
        n = n.substring(0, dot);
      dest = Key.make(n + Extensions.KMEANS);
    }

    return hex.KMeans.start(dest, va, k, ep, maxIter, seed_, norm, columns);
  }
}
