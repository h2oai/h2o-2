package water;

/**
 * Runs a map reduce task over a dataset and sums elements of a column.
 */
public class Part04_MapReduce {
  // Ignore this boilerplate main, c.f. previous samples
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserMain.class, args);
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
      H2O.main(args);
      Key key = TestUtil.loadAndParseFile("test", "smalldata/gaussian/sdss174052.csv.gz");

      Sum sum = new Sum();
      sum._mainKey = key;
      sum.invoke(key);

      System.out.println("Sum is " + sum._value);
    }
  }

  /**
   * Runs in parallel using distributed fork/join. Each thread on each <br>
   * machines sees a chunk of the dataset.
   */
  static class Sum extends MRTask {
    Key _mainKey;
    double _value;

    @Override public void map(Key chunkKey) {
      assert chunkKey.home();
      // Ignore, boilerplate code scheduled for simplification
      ValueArray va = DKV.get(_mainKey).get();
      AutoBuffer bits = va.getChunk(chunkKey);
      int rows = va.rpc(ValueArray.getChunkIndex(chunkKey));

      // Iterate over all rows for this chunk, and sums elements
      for( int row = 0; row < rows; row++ ) {
        // Pick elements of first column
        ValueArray.Column c = va._cols[0];
        double d = va.datad(bits, row, c);
        _value += d;
      }
    }

    @Override public void reduce(DRemoteTask rt) {
      _value += ((Sum) rt)._value;
    }
  }
}
