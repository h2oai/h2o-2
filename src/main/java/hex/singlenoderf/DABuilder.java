package hex.singlenoderf;


import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
import water.Key;
import water.Timer;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.util.Log;

import java.util.ArrayList;


public class DABuilder {
    protected final SpeeDRF _drf;
    protected final SpeeDRFModel _rfmodel;

    static DABuilder create(final SpeeDRF drf, final SpeeDRFModel rf_model) {
      switch( drf.drfParams._samplingStrategy ) {
        case RANDOM                :
        case STRATIFIED_LOCAL      :
        default                    : return new DABuilder(drf, rf_model);
      }
    }

    @SuppressWarnings("unused") private DABuilder() { this(null, null);}

    DABuilder(final SpeeDRF drf, final SpeeDRFModel rf_model) { _drf = drf; _rfmodel = rf_model; }

    final DataAdapter build(Frame fr) { return inhaleData(fr); }

    /** Check that we have proper number of valid columns vs. features selected, if not cap*/
    private void checkAndLimitFeatureUsedPerSplit() {
      int validCols = _drf.source.numCols()-1; // for classIdx column
      if (validCols < _drf.drfParams._numSplitFeatures) {
        Log.info(Log.Tag.Sys.RANDF, "Limiting features from " + _drf.drfParams._numSplitFeatures +
                " to " + validCols + " because there are no more valid columns in the dataset");
        _drf.drfParams._numSplitFeatures= validCols;
      }
    }

    /** Return the number of rows on this node. */
    private int getRowCount(Frame fr) {
      return (int)fr.numRows();
    }

    /** Return chunk index of the first chunk on this node. Used to identify the trees built here.*/
    private long getChunkId(final Frame fr) {
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
      SpeeDRFModel rfmodel = _rfmodel;
      boolean[] _isByteCol = new boolean[fr.numCols()];
      for (int i = 0; i < _isByteCol.length; ++i) {
        _isByteCol[i] = DataAdapter.isByteCol(fr.vecs()[i], (int)fr.numRows(), i == _isByteCol.length - 1);
      }
      // The model columns are dense packed - but there will be columns in the
      // data being ignored.  This is a map from the model's columns to the
      // building dataset's columns.
      final int[] modelDataMap = colMap(fr._names, rfmodel._names);
      final int totalRows = getRowCount(fr);
      final DataAdapter dapt = new DataAdapter(fr, rfmodel, modelDataMap,
              totalRows,
              getChunkId(fr),
              _drf.drfParams._seed,
              _drf.drfParams._binLimit,
              _drf.drfParams._classWt);
      // Check that we have proper number of valid columns vs. features selected, if not cap.
      checkAndLimitFeatureUsedPerSplit();

      // Collects jobs loading local chunks
      ArrayList<RecursiveAction> dataInhaleJobs = new ArrayList<RecursiveAction>();
      for(int i = 0; i < fr.anyVec().nChunks(); ++i) {
        dataInhaleJobs.add(loadChunkAction(dapt, fr, i, _isByteCol));
      }
      _rfmodel.current_status = "Inhaling Data";
      _rfmodel.update(_rfmodel.jobKey);
      ForkJoinTask.invokeAll(dataInhaleJobs);

      // Shrink data
      dapt.shrink();
      Log.info(Log.Tag.Sys.RANDF,"Inhale done in " + t_inhale);
      return dapt;
    }

    static RecursiveAction loadChunkAction(final DataAdapter dapt, final Frame fr, final int cidx, final boolean[] isByteCol) {
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
                if(chks[c].isNA0(j)) {
                  if (c == ncolumns - 1) rowIsValid = false;
                  dapt.addBad(rowNum, c); continue;
                }
                if (isByteCol[c]) {
                  int val = (int)chks[c].at8(rowNum);
                  dapt.add1(val, rowNum, c);
                } else {
                  float f = (float)chks[c].at(rowNum);
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