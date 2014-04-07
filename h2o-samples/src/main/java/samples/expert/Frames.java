package samples.expert;

import water.Futures;
import water.Job;
import water.Key;
import water.UKV;
import water.deploy.VM;
import water.fvec.*;

import java.io.File;

/**
 * Demonstration of H2O's Frame API, the distributed table-like data structure.
 */
public class Frames extends Job {
  public static void main(String[] args) throws Exception {
    Class job = Frames.class;
    samples.launchers.CloudLocal.launch(job, 1);
    //samples.launchers.CloudProcess.launch(job, 2);
    //samples.launchers.CloudConnect.launch(job, "localhost:54321");
    //samples.launchers.CloudRemote.launchIPs(job, "192.168.1.161", "192.168.1.162");
    //samples.launchers.CloudRemote.launchEC2(job, 4);
  }

  @Override
  protected void execImpl() {
    // From file
    parse(new File(VM.h2oFolder(), "smalldata/iris/iris.csv"));

    // Programmatically
    Frame frame = create( //
        new String[] { "A", "B" }, //
        new double[][] { //
        new double[] { 1.0, 2.0 }, //
        new double[] { 3.0, 4.0 } });

    // Store frame in H2O's K/V store
    Key key = Key.make("MyFrame");
    UKV.put(key, frame);
  }

  /**
   * Parse a dataset into a Frame.
   */
  public static Frame parse(File file) {
    Key fkey = NFSFileVec.make(file);
    Key dest = Key.make(file.getName());
    Frame frame = ParseDataset2.parse(dest, new Key[] { fkey });
    return frame;
  }

  /**
   * Creates a frame programmatically.
   */
  public static Frame create(String[] headers, double[][] rows) {
    Futures fs = new Futures();
    Vec[] vecs = new Vec[rows[0].length];
    Key keys[] = new Vec.VectorGroup().addVecs(vecs.length);
    for( int c = 0; c < vecs.length; c++ ) {
      AppendableVec vec = new AppendableVec(keys[c]);
      NewChunk chunk = new NewChunk(vec, 0);
      for( int r = 0; r < rows.length; r++ )
        chunk.addNum(rows[r][c]);
      chunk.close(0, fs);
      vecs[c] = vec.close(fs);
    }
    fs.blockForPending();
    return new Frame(headers, vecs);
  }
}
