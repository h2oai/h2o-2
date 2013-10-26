package water;

/**
 * Runs a map reduce task over a dataset and sums elements of a column.
 */
public class Sample05_MapReduce {
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserCode.class, args);
  }

  public static class UserCode {
    public static void userMain(String[] args) throws Exception {
      /*
       * Starts an H2O instance in the local JVM. This method is the one invoked by default when no
       * '-mainClass' parameter is specified.
       */
      H2O.main(args);

      /*
       * Convenience method to load, uncompress and parse a file. The data is distributed in chunks
       * to all nodes during the process. The overall data structure containing all chunks is
       * accessible in H2O's distributed KV store at the 'test' key.
       */
      Key key = TestUtil.loadAndParseFile("test", "smalldata/gaussian/sdss174052.csv.gz");

      /*
       * Create an instance of our custom map-reduce class.
       */
      Sum sum = new Sum();

      /*
       * Any field set before invoking the task will be copied to other instances created for local
       * threads, and serialized to remote instances used on remote nodes.
       */
      sum._mainKey = key;

      /*
       * Launches a distributed fork-join that will create instances of the task, and run them in
       * parallel on each chunk of data for this key.
       */
      sum.invoke(key);

      /*
       * At this point, all task instances have been merged by their 'reduce' method. We are back to
       * a state where only one instance exist, and it contains the overall sum.
       */
      System.out.println("Sum is " + sum._value);
    }
  }

  /**
   * Distributed fork/join task that sums elements of the first column of a dataset.
   */
  static class Sum extends MRTask {
    /*
     * This field is only set before the task runs, so it will be copied to all instance of the
     * task, and remain constant during a run. It can be seen as an input field.
     */
    Key _mainKey;

    /*
     * This field is updated by the task, and needs to be reduced between instances. It can be seen
     * as an output field.
     */
    double _value;

    @Override public void map(Key chunkKey) {
      assert chunkKey.home();
      /*
       * Gets the ValueArray, a data structure representing the whole dataset. Getting this object
       * doesn't fetch the data, it is only a pointer to chunks distributed over cluster nodes.
       * (Warning, the following three line are mostly boilerplate and scheduled for
       * simplification.)
       */
      ValueArray va = DKV.get(_mainKey).get();

      /*
       * Get the buffer for the chunk this MRTask instance will run on, and its number of rows.
       */
      AutoBuffer bits = va.getChunk(chunkKey);
      int rows = va.rpc(ValueArray.getChunkIndex(chunkKey));

      // Iterate over all rows for this chunk, and sums elements
      for( int row = 0; row < rows; row++ ) {
        /*
         * Get the first column for this dataset, and get the element for this row.
         */
        ValueArray.Column c = va._cols[0];
        double d = va.datad(bits, row, c);
        _value += d;
      }
    }

    @Override public void reduce(DRemoteTask rt) {
      /*
       * This operation will be invoked for each MRTask, to add together sums for each chunk.
       */
      _value += ((Sum) rt)._value;
    }
  }
}
