package hex.rf;

import java.util.ArrayList;

import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
import water.*;
import water.util.Utils;

class DABuilder {

  protected final DRF _drf;

  static DABuilder create(final DRF drf) {
    return drf._useStratifySampling ? new StratifiedDABuilder(drf) : new DABuilder(drf);
  }

  @SuppressWarnings("unused") private DABuilder() { this(null); };

  DABuilder(final DRF drf) { _drf = drf;  }

  final DataAdapter build(Key [] keys) { return inhaleData(keys); }

  /** Check that we have proper number of valid columns vs. features selected, if not cap*/
  final void checkAndLimitFeatureUsedPerSplit(final DataAdapter dapt) {
    int validCols = _drf._rfmodel._va._cols.length-1; // for classIdx column
    if (validCols < _drf._numSplitFeatures) {
      Utils.pln("Limiting features from " + _drf._numSplitFeatures +
          " to " + validCols + " because there are no more valid columns in the dataset");
      _drf._numSplitFeatures= validCols;
    }
  }

  /** Return the number of rows on this node. */
  final int getRowCount(final Key[] keys) {
    int num_rows = 0;    // One pass over all chunks to compute max rows
    ValueArray ary = DKV.get(_drf._rfmodel._dataKey).get();
    for( Key key : keys ) if( key.home() ) num_rows += ary.rpc(ValueArray.getChunkIndex(key));
    return num_rows;
  }

  /** Return chunk index of the first chunk on this node. Used to identify the trees built here.*/
  final long getChunkId(final Key[] keys) {
    for( Key key : keys ) if( key.home() ) return ValueArray.getChunkIndex(key);
    throw new Error("No key on this node");
  }

  /** Build data adapter for given array */
  protected  DataAdapter inhaleData(Key [] keys) {
    Timer t_inhale = new Timer();
    RFModel rfmodel = _drf._rfmodel;
    final ValueArray ary = DKV.get(rfmodel._dataKey).get();

    // The model columns are dense packed - but there will be columns in the
    // data being ignored.  This is a map from the model's columns to the
    // building dataset's columns.
    final int[] modelDataMap = rfmodel.columnMapping(ary.colNames());

    final DataAdapter dapt = new DataAdapter( ary, rfmodel, modelDataMap,
                                              getRowCount(keys),
                                              getChunkId(keys),
                                              _drf._seed,
                                              _drf._binLimit,
                                              _drf._classWt);
    // Check that we have proper number of valid columns vs. features selected, if not cap.
    checkAndLimitFeatureUsedPerSplit(dapt);
    // Now load the DataAdapter with all the rows on this node.
    final int ncolumns = rfmodel._va._cols.length;

    ArrayList<RecursiveAction> dataInhaleJobs = new ArrayList<RecursiveAction>();
    int start_row = 0;
    for( final Key k : keys ) {    // now read the values
      final int S = start_row;
      if (!k.home()) continue;     // skip no local keys (we only inhale local data)
      final int rows = ary.rpc(ValueArray.getChunkIndex(k));
      dataInhaleJobs.add(new RecursiveAction() {
        @Override protected void compute() {
          AutoBuffer bits = ary.getChunk(k);
          for(int j = 0; j < rows; ++j) {
            int rowNum = S + j; // row number in the subset of the data on the node
            boolean rowIsValid = false;
            for( int c = 0; c < ncolumns; ++c) { // For all columns being processed
              final int col = modelDataMap[c];   // Column in the dataset
              if( ary.isNA(bits,j,col) ) { dapt.addBad(rowNum, c); continue; }
              float f =(float)ary.datad(bits,j,col);
              if( !dapt.isValid(c,f) ) { dapt.addBad(rowNum, c); continue; }
              dapt.add(f, rowNum, c);
              // if the row contains at least one correct value except class
              // column consider it as correct
              if( c != ncolumns-1 )
                rowIsValid |= true;
            }
            // The whole row is invalid in the following cases: all values are NaN or there is no class specified (NaN in class column)
            if (!rowIsValid) dapt.markIgnoredRow(j);
          }
        }});
      start_row += rows;
    }
    ForkJoinTask.invokeAll(dataInhaleJobs);
    dapt.shrink();
    Utils.pln("[RF] Inhale done in " + t_inhale);
    return dapt;
  }
}
