package hex.speedrf;

import hex.speedrf.DRF.DRFTask;
import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
import water.*;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.util.Log;
import water.util.Log.Tag.Sys;

import java.util.ArrayList;

class DABuilder {
  protected final DRFTask _drf;

  static DABuilder create(final DRFTask drf) {
    switch( drf._params._samplingStrategy ) {
    case RANDOM                :
    case STRATIFIED_LOCAL      :
    default                    : return new DABuilder(drf);
    }
  }

  @SuppressWarnings("unused") private DABuilder() { this(null); };

  DABuilder(final DRFTask drf) { _drf = drf;  }

  final DataAdapter build(Frame fr) { return inhaleData(_drf._rfmodel._fr); }

  /** Check that we have proper number of valid columns vs. features selected, if not cap*/
  private final void checkAndLimitFeatureUsedPerSplit(final DataAdapter dapt) {
    int validCols = _drf._rfmodel._fr.numCols()-1; // for classIdx column
    if (validCols < _drf._params._numSplitFeatures) {
      Log.warn(Sys.RANDF,"Limiting features from " + _drf._params._numSplitFeatures +
          " to " + validCols + " because there are no more valid columns in the dataset");
      _drf._params._numSplitFeatures= validCols;
    }
  }

  /** Return the number of rows on this node. */
  private final int getRowCount(Frame fr) {
    return (int)fr.numRows();
  }

  /** Return chunk index of the first chunk on this node. Used to identify the trees built here.*/
  private final long getChunkId(final Frame fr) {
    Key[] keys = new Key[fr.anyVec().nChunks()];
    for(int i = 0; i < fr.anyVec().nChunks(); ++i) {
      keys[i] = fr.anyVec().chunkKey(i);
    }
    for(int i = 0; i < keys.length; ++i) {
      if (keys[i].home()) return i;
    }
    throw new Error("No key on this node");
  }

  private static int find(String n, String[] names) {
    if( n == null ) return -1;
    for( int j = 0; j<names.length; j++ )
      if( n.equals(names[j]) )
        return j;
    return -1;
  }

  public static int[] colMap( String[] frame_names, String[] model_names ) {
    int mapping[] = new int[frame_names.length];
    for( int i = 0; i<mapping.length; i++ )
      mapping[i] = find(frame_names[i],model_names);
    return mapping;
  }

  /** Build data adapter for given frame */
  protected DataAdapter inhaleData(Frame fr) {
    Timer t_inhale = new Timer();
    RFModel rfmodel = _drf._rfmodel;


    // The model columns are dense packed - but there will be columns in the
    // data being ignored.  This is a map from the model's columns to the
    // building dataset's columns.
    final int[] modelDataMap = colMap(fr._names, rfmodel._names);
    final int totalRows = getRowCount(fr);
    final DataAdapter dapt = new DataAdapter( fr, rfmodel, modelDataMap,
                                              totalRows,
                                              getChunkId(fr),
                                              _drf._params._seed,
                                              _drf._params._binLimit,
                                              _drf._params._classWt);
    // Check that we have proper number of valid columns vs. features selected, if not cap.
    checkAndLimitFeatureUsedPerSplit(dapt);
    // Now load the DataAdapter with all the rows on this node.
    final int ncolumns = fr.numCols();

    // Collects jobs loading local chunks
    ArrayList<RecursiveAction> dataInhaleJobs = new ArrayList<RecursiveAction>();
    for(int i = 0; i < fr.anyVec().nChunks(); ++i) {
      dataInhaleJobs.add(loadChunkAction(dapt, fr, i, modelDataMap, totalRows));
    }
    ForkJoinTask.invokeAll(dataInhaleJobs);

    // Shrink data
    dapt.shrink();
    Log.debug(Sys.RANDF,"Inhale done in " + t_inhale);
    return dapt;
  }

  static RecursiveAction loadChunkAction(final DataAdapter dapt, final Frame fr, final int cidx, final int[] modelDataMap, final int totalRows) {
    return new RecursiveAction() {
      @Override protected void compute() {
        try {
          Chunk[] chks = new Chunk[fr.numCols()];
          int ncolumns = chks.length;
          for(int i = 0; i < chks.length; ++i) { chks[i] = fr.vecs()[i].chunkForChunkIdx(cidx); }
          for (int j = 0; j < chks[0]._len; ++j) {
            int rowNum = (int)chks[0]._start + j;
            boolean rowIsValid = false;
            for(int c = 0; c < chks.length; ++c) {
              //final int col = modelDataMap[c];
              if(chks[c].isNA(j)) {
                if (c == ncolumns - 1) rowIsValid = false;
                dapt.addBad(rowNum, c); continue;
              }
              if (DataAdapter.isByteCol(fr.vecs()[c], totalRows, c == ncolumns - 1)) {
                int val = (int)chks[c].at8(j);
                dapt.add1(val, rowNum, c);
              } else {
                float f = (float)chks[c].at(j);
                if(!dapt.isValid(c, f)) { dapt.addBad(rowNum, c); continue; }
                dapt.add(f, rowNum, c);
              }
              if (c != ncolumns - 1) {
                rowIsValid |= true;
              }

            }
            if (!rowIsValid) dapt.markIgnoredRow(j);
          }
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
    };
  }
}