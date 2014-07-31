package water.util;

import java.io.File;

import water.Futures;
import water.Key;
import water.fvec.*;

public class FrameUtils {

  /** Create a frame with single column represented by given vector.
   *
   * @param name  name of the column
   * @param vec  column data
   * @return a new frame
   */
  public static Frame frame(String name, Vec vec)       { return new Frame().add(name, vec); }
  /**
   * Create a new frame based on given column data.
   * @param names  name of frame columns
   * @param vecs  columns data represented by individual data
   * @return a new frame composed of given vectors.
   */
  public static Frame frame(String[] names, Vec[] vecs) { return new Frame(names, vecs); }
  /**
   * Create a new frame based on given row data.
   * @param names  names of frame columns
   * @param rows  data given in the form of rows
   * @return new frame which contains columns named according given names and including given data
   */
  public static Frame frame(String[] names, double[]... rows) {
    assert names == null || names.length == rows[0].length;
    Futures fs = new Futures();
    Vec[] vecs = new Vec[rows[0].length];
    Key keys[] = Vec.VectorGroup.VG_LEN1.addVecs(vecs.length);
    for( int c = 0; c < vecs.length; c++ ) {
      AppendableVec vec = new AppendableVec(keys[c]);
      NewChunk chunk = new NewChunk(vec, 0);
      for( int r = 0; r < rows.length; r++ )
        chunk.addNum(rows[r][c]);
      chunk.close(0, fs);
      vecs[c] = vec.close(fs);
    }
    fs.blockForPending();
    return new Frame(names, vecs);
  }

  /** Parse given file into the form of frame represented by the given key.
   *
   * @param okey  destination key for parsed frame
   * @param files  files to parse
   * @return a new frame
   */
  public static Frame parseFrame(Key okey, File ...files) {
    assert files.length > 0 : "Ups. No files to parse!";
    for (File f : files)
      if (!f.exists())
        throw new RuntimeException("File not found " + f);
    // Create output key if not specified
    if(okey == null)
      okey = Key.make(files[0].getName());

    Key[] fkeys = new Key[files.length];
    int cnt = 0;
    for (File f : files) fkeys[cnt++] = NFSFileVec.make(f);
    return parseFrame(okey, fkeys);
  }

  public static Frame parseFrame(Key okey, Key ...ikeys) {
    assert okey != null;
    return ParseDataset2.parse(okey, ikeys);
  }

  /**
   * Compute a chunk summary (how many chunks of each type, relative size, total size)
   * @param fr
   * @return chunk summary
   */
  public static ChunkSummary chunkSummary(Frame fr) {
    return new ChunkSummary().doAll(fr);
  }
}
