package samples;

import java.io.File;
import java.text.DecimalFormat;
import java.util.Random;

import water.*;
import water.fvec.*;
import water.util.Utils;

/**
 * Simplified version of H2O k-means algorithm for better readability.
 */
public class Sample05_Sum {
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, args);
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      H2O.main(args);

      // Load and parse a file. Data is distributed to other nodes in a round-robin way
      Key file = NFSFileVec.make(new File("lib/resources/datasets/gaussian.csv"));
      Frame frame = ParseDataset2.parse(Key.make("test"), new Key[] { file });

      Sum task = new Sum();
      task.doAll(frame);
      System.out.println("Sum is " + task.getSum());
      System.exit(0);
    }
  }

  public static class Sum extends MRTask2<Sum> {
    private double _totalSum;
    private double _partialSum;

    double getSum() { return _totalSum; }

    @Override public void map(Chunk[] chunks) {
      // Find nearest cluster for each row
      _partialSum = 0;
      for( int row = 0; row < chunks[0]._len; row++ ) {
        double value = chunks[0].at0(row);
        _partialSum += value;
      }
      _totalSum += _partialSum;
    }

    @Override public void reduce(Sum rhs) {
      _totalSum += rhs._totalSum;
    }
  }
}
