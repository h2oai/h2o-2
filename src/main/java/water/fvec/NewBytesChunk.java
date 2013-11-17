package water.fvec;

/**
 * A new chunk of bytes.
 * Convenient for reading in a binary file from a data source and storing it as a vector.
 *
 * Currently this class is really a placeholder for a future optimization,
 * since we know ahead of time that the chunks should hold bytes.
 */
public class NewBytesChunk extends NewChunk {
  /**
   * Constructor.
   */
  public NewBytesChunk(Vec vec, int cidx) { super(vec, cidx); }

  /**
   * Append a byte to this chunk.
   */
  public void addByte(byte val) {
    long l = val;
    l = l & 0xff;
    addNum(l, 0);
  }
}
