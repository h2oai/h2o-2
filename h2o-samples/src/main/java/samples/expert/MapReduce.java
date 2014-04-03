package samples.expert;

import org.junit.Assert;
import water.Job;
import water.MRTask2;
import water.deploy.VM;
import water.fvec.Chunk;
import water.fvec.Frame;

import java.io.File;

/**
 * Demonstration of H2O's map-reduce API. This task sums the elements of a column.
 */
public class MapReduce extends Job {
  public static void main(String[] args) throws Exception {
    Class job = MapReduce.class;
    samples.launchers.CloudLocal.launch(job, 1);
    //samples.launchers.CloudProcess.launch(job, 2);
    //samples.launchers.CloudConnect.launch(job, "localhost:54321");
    //samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.162");
    //samples.launchers.CloudRemote.launchEC2(job, 4);
  }

  @Override
  protected void execImpl() {
    // Parse a dataset into a Frame, H2O's distributed table-like data structure
    File file = new File(VM.h2oFolder(), "smalldata/iris/iris.csv");
    Frame frame = samples.expert.Frames.parse(file);

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
    System.out.println("Sum is " + sum.value);
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
    double value;

    /**
     * This method is invoked on each chunk of the distributed data structure.
     */
    @Override
    public void map(Chunk chunk) {
      Assert.assertEquals("blah", myInput);

      for( int row = 0; row < chunk._len; row++ )
        value += chunk.at0(row);

      // Optionally, setting inputs to null if not needed anymore avoids
      // their serialization back to the initiating node
      myInput = null;
    }

    /**
     * This operation will be invoked for each MRTask, to add together sums for each chunk.
     */
    @Override
    public void reduce(Sum other) {
      value += other.value;
    }
  }
}
