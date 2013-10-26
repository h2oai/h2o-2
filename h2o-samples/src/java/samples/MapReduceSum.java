package samples;

import java.io.File;

import org.junit.Assert;

import water.*;
import water.fvec.*;

/**
 * Runs a map reduce task over a dataset to sum elements of a column.
 */
public class MapReduceSum extends Job {
  public static void main(String[] args) throws Exception {
    //CloudLocal1.launch(MapReduceSum.class);
    CloudLocal4.launch(4, true, MapReduceSum.class);
  }

  @Override protected void exec() {
    // Parse a dataset into a Frame, H2O's distributed table-like data structure
    File file = new File("../smalldata/iris/iris.csv");
    Key fkey = NFSFileVec.make(file);
    Frame frame = ParseDataset2.parse(Key.make(file.getName()), new Key[] { fkey });

    // Create an instance of our custom map-reduce class.
    Sum sum = new Sum();

    // Any field set before invoking the task will be copied to other instances created
    // for local threads, and serialized to remote instances used on remote nodes.
    sum.myInput = "blah";

    // Launches a distributed fork-join that will create instances of the task, and run
    // them in parallel on each chunk of data for this key. In this example, run on only
    // on one column, the second one of the frame.
    sum.doAll(frame.vecs()[1]);

    // At this point, all task instances have been merged by their 'reduce' method. We
    // are back to a state where only one instance exist, and it contains the overall sum.
    System.out.println("Sum is " + sum._value);
  }

  static class Sum extends MRTask2<Sum> {
    /**
     * This field is only set before the task runs, so it will be copied to all instance of the
     * task, and remain constant during a run. It can be seen as an input field.
     */
    String myInput;

    /**
     * This field is updated by the task, and needs to be reduced between instances. It can be seen
     * as an output field.
     */
    double _value;

    /**
     * This method is invoked on each chunk of the distributed data structure.
     */
    @Override public void map(Chunk chunk) {
      Assert.assertEquals("blah", myInput);

      for( int row = 0; row < chunk._len; row++ )
        _value += chunk.at0(row);

      // Optionally, setting inputs to null if not needed anymore avoids
      // their serialization back to the initiating node
      myInput = null;
    }

    /**
     * This operation will be invoked for each MRTask, to add together sums for each chunk.
     */
    @Override public void reduce(Sum other) {
      _value += other._value;
    }
  }
}
