/**
 *
 */
package hex;

import java.util.Arrays;

import water.H2O.H2OCountedCompleter;
import water.*;
import water.fvec.*;
import water.util.Utils;

/**
 *
 */
public class NFoldFrameExtractor extends FrameExtractor {

  /** Number of folds */
  final int nfolds;

  /** Active fold which will be extracted. */
  final int afold;

  public NFoldFrameExtractor(Frame dataset, int nfolds, int afold, Key[] destKeys, Key jobKey) {
    super(dataset, destKeys, jobKey);
    assert afold >= 0 && afold < nfolds : "afold parameter is out of bound <0,nfolds)";
    this.nfolds = nfolds;
    this.afold  = afold;
  }

  @Override protected MRTask2 createNewWorker(H2OCountedCompleter completer, Vec[] inputVecs, int split) {
    assert split == 0 || split == 1;
    return new FoldExtractTask(completer, inputVecs, nfolds, afold, split==1);
  }

  @Override protected long[][] computeEspcPerSplit(long[] espc, long nrows) {
    assert espc[espc.length-1] == nrows : "Total number of rows does not match!";
    long[] ith = Utils.nfold(nrows, nfolds, afold); // Compute desired fold position
    long startRow = ith[0], endRow = startRow + ith[1];
    long[][] r = new long[2][espc.length+1]; // In the worst case we will introduce a new chunk
    int c1 = 0, c2 = 0; // Number of chunks in each partition
    long p1rows = 0, p2rows = 0;
    int c = 0; // Chunk idx
    // Extract the first section of the remaining part
    for (; c<espc.length-1 && espc[c+1] <= startRow; c++) p1rows = r[0][++c1] = espc[c+1]; // Find the chunk with the split
    // c is chunk which needs a split between remaining part and selected fold, but it can be split into 3 pieces as well!
    if (r[0][c1] < (p1rows += (startRow-espc[c]))) r[0][++c1] = p1rows; // Start for new chunk of part1
    // Now extract i-th fold
    for (; c<espc.length-1 && espc[c+1] <= endRow; c++ ) p2rows = r[1][++c2] = espc[c+1]-startRow;
    if (r[1][c2] < (p2rows += (endRow-Math.max(espc[c],startRow)))) r[1][++c2] = p2rows;
    assert p2rows == ith[1];
    // Extract rest
    for (; c<espc.length-1; c++) p1rows = r[0][++c1] = espc[c+1]-ith[1];
    r[0] = Arrays.copyOf(r[0], c1+1);
    r[1] = Arrays.copyOf(r[1], c2+1);
    // Post-conditions
    assert r[0][r[0].length-1]+r[1][r[1].length-1] == nrows;
    return r;
  }

  @Override protected int numOfOutputs() {
    return 2;
  }

  private static class FoldExtractTask extends MRTask2<FoldExtractTask> {
    private final Vec [] _vecs; // source vectors
    private final int _nfolds;
    private final int _afold;
    private final boolean _inFold;

    transient int _precedingChks;   // number of preceding chunks
    transient int _startFoldChkIdx; // idx of 1st chunk for the fold
    transient int _startRestChkIdx; // idx of 1st of remaining part
    transient int _startFoldRow;  // fold start row inside the chunk _startFoldChkIdx
    transient int _startRestRow;  // index of the 1st row inside chunk _startRestChkIdx begining remaining part of data

    @Override protected void setupLocal() {
      Vec anyInVec = _vecs[0];
      long[] folds = Utils.nfold(anyInVec.length(), _nfolds, _afold);
      long startRow = folds[0];
      long endRow  = startRow+folds[1];
      long espc[] = anyInVec._espc;
      int c = 0;
      for (; c<espc.length-1 && espc[c+1] <= startRow; c++) ;
      _startFoldChkIdx = c;
      _startFoldRow = (int) (startRow-espc[c]);
      _precedingChks = _startFoldRow > 0 ? c+1 : c;
      for (; c<espc.length-1 && espc[c+1] <= endRow; c++) ;
      _startRestChkIdx = c;
      _startRestRow = (int) (endRow-espc[c]);
    }
    public FoldExtractTask(H2OCountedCompleter completer, Vec[] srcVecs, int nfold, int afold, boolean inFold) {
      super(completer);
      _vecs = srcVecs;
      _nfolds = nfold;
      _afold = afold;
      _inFold = inFold;
    }
    @Override public void map(Chunk[] cs) {
      int coutidx = cs[0].cidx(); // output chunk where to extract
      int cinidx  = getInChunkIdx(coutidx); // input chunk where to extract
      int startRow = getStartRow(coutidx); // start row for extraction
      int nrows = cs[0]._len; // number of rows to extract from the input chunk
      for (int i=0; i<cs.length; i++) {
        ChunkSplitter.extractChunkPart(_vecs[i].chunkForChunkIdx(cinidx), cs[i], startRow, nrows, _fs);
      }
    }
    private int getInChunkIdx(int coutidx) {
      if (_inFold)
        return _startFoldChkIdx==_startRestChkIdx ? _startFoldChkIdx : coutidx + _startFoldChkIdx;
      else { // out fold part
        if (coutidx < _precedingChks)
          return coutidx;
        else
          return _startRestChkIdx + (coutidx-_precedingChks);
      }
    }
    private int getStartRow(int coutidx) {
      if (_inFold)
        return coutidx == 0 ? _startFoldRow : 0;
      else { //out fold part
        return coutidx == _precedingChks ? _startRestRow : 0;
      }
    }
  }
}
