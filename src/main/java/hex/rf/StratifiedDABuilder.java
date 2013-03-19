package hex.rf;

import hex.rf.MinorityClasses.UnbalancedClass;

import java.util.*;

import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
import water.*;
import water.ValueArray.Column;
import water.util.Utils;

public class StratifiedDABuilder extends DABuilder {

  StratifiedDABuilder(DRF drf) {
    super(drf);
  }

  /**
   * Job to inhale data for stratify sampling.
   *
   * It is done differently than standard inhale since we sort the data by classes.
   */
  static final class DataInhale extends RecursiveAction {
    final ValueArray  _ary;
    final DataAdapter _dapt;
    final int[]       _modelDataMap;
    final int[]       _startRows;
    final Key         _k;
    final boolean[]   _iclasses;
    final int[] _binColIds;
    final int[] _rawColIds;
    final int[] _rawColMins;
    final int   _nclasses;
    boolean     _bin;

    public DataInhale(final ValueArray ary, final DataAdapter dapt, RFModel rf,
        final int[] startRows, final Key k, boolean bin, final boolean[] iclasses,
        final int [] binColIds, final int [] rawColIds, final int [] rawColMins
        ) {
      _ary = ary;
      _dapt = dapt;
      _startRows = startRows;
      _k = k;
      _bin = bin;
      _iclasses = iclasses;
      _binColIds = binColIds;
      _rawColIds = rawColIds;
      _rawColMins = rawColMins;
      _nclasses = rf.classes();
      _modelDataMap = rf.columnMapping(ary.colNames());
    }
    public DataInhale(DataInhale o, RFModel rf){
      this(o._ary, o._dapt, rf, o._startRows, o._k, o._bin, o._iclasses,
           o._binColIds, o._rawColIds, o._rawColMins);
    }

    @Override
    protected void compute() {
      AutoBuffer bits    = _ary.getChunk(_k);
      Column     cl      = _ary._cols[_modelDataMap[_modelDataMap.length-1]];
      int        rows    = bits.remaining()/_ary.rowSize();
      int []     indexes = new int[_nclasses];

      ROWS:for(int i = 0; i < rows; ++i){
        int c = (int)(_ary.data(bits, i, cl)-cl._min);
        int outputRow = indexes[c] + _startRows[c];

        if( (_iclasses != null) && _iclasses[c] )
          continue ROWS;
        for( int col:_binColIds) {
          int dcol = _modelDataMap[col];
          if( _ary.isNA(bits,i,dcol) ) { /*markBadValue?*/continue ROWS; }
          float f =(float)_ary.datad(bits,i,dcol);
          if( !_dapt.isValid(col,f) ) { /*_dapt.markBadValue(outputRow, col)*/ continue ROWS; }
        }
        for( int col:_rawColIds) {
          int dcol = _modelDataMap[col];
          if( _ary.isNA(bits,i,dcol) ) { /*markBadValue?*/continue ROWS; }
          float f =(float)_ary.datad(bits,i,dcol);
          if( !_dapt.isValid(col,f) ) { /*_dapt.markBadValue(outputRow, col)*/ continue ROWS; }
        }
        ++indexes[c];

        if(_bin){
          for(int col:_binColIds)
      ;//      _dapt.addValueRaw((float)_ary.datad(bits, i, col),outputRow,col);
        } else {
          for(int col:_binColIds)
         ;//   _dapt.addValue((float)_ary.datad(bits, i, col), outputRow, col);
          for(int col = 0; col < _rawColIds.length; ++col){
         ;//   _dapt.addValue((short)(_ary.data(bits, i, _rawColIds[col]) - _rawColMins[col]), outputRow, _rawColIds[col]);
          }
        }
      }
      _bin = false;
    }
  }

  /**
   * Data inhale for stratify sampling.
   *
   * Sorts input by the class column and sample
   * data from other nodes in case of minority/unbalanced class.
   */
  @Override
  protected final DataAdapter inhaleData(final Key [] keys) {
    final ValueArray ary = DKV.get(_drf._rfmodel._dataKey).get();
    int   row_size        = ary.rowSize();
    int   rpc             = (int)ValueArray.CHUNK_SZ/row_size;
    final Column classCol = ary._cols[_drf._classcol];
    final int    nclasses = _drf._rfmodel.classes();
    boolean []   unbalancedClasses = null;
    final int[] modelDataMap = _drf._rfmodel.columnMapping(ary.colNames());

    final int [][] chunkHistogram = new int [keys.length+1][nclasses];

    // first compute histogram of classes per chunk (for sorting purposes)
    RecursiveAction [] htasks = new RecursiveAction[keys.length];
    for(int i = 0; i < keys.length; ++i){
      final int chunkId  = i;
      final Key chunkKey = keys[i];

      htasks[i] = new RecursiveAction() {
        @Override
        protected void compute() {
          AutoBuffer bits = ary.getChunk(chunkKey);
          int rows = bits.remaining()/ary.rowSize();
          for(int i = 0; i < rows; ++i) {
           // FIXME:   if( !_dapt.ignore(col) && ! _dapt.isValid(_ary,bits, i, col) )
            if (!ary.isNA(bits, i, classCol))
              ++chunkHistogram[chunkId][(int)(ary.data(bits, i, classCol)-classCol._min)];
          }
        }
      };
    }
    ForkJoinTask.invokeAll(htasks);
    // compute sums of our class counts
    for(int i = 0; i < keys.length; ++i)
      for(int j = 0; j < nclasses; ++j)
        chunkHistogram[keys.length][j] += chunkHistogram[i][j];

    ArrayList<Key> myKeys = new ArrayList<Key>();
    for(Key k : keys)myKeys.add(k);
    if(_drf._uClasses != null) {
      // boolean array to keep track which classes to ignore when reading local keys
      unbalancedClasses = new boolean[nclasses];
      for(UnbalancedClass c:_drf._uClasses){
        unbalancedClasses[c._c] = true;
        int nrows = _drf._strata[c._c];
        int echunks = 1 + nrows/rpc;
        if(echunks >= c._chunks.length) { // we need all the chunks from all the nodes
          chunkHistogram[keys.length][c._c] = _drf._gHist[c._c];
          for(Key k:c._chunks)
            myKeys.add(k);
        } else { // sample only from some of chunks on other nodes
          int r = 0;
          ArrayList<Integer> indexes = new ArrayList<Integer>();
          // include all local chunks and identify non-locals
          for(int i = 0; i < c._chunks.length; ++i) {
            if(c._chunks[i].home()){
              myKeys.add(c._chunks[i]);
              r += DKV.get(c._chunks[i])._max/row_size;
            } else
              indexes.add(i);
          }
          // sample from non-local chunks until we have enough rows
          // sampling only works on chunk boundary -> we can end up with upt to rpc more rows than requested
          Random rand = Utils.getRNG(_drf._seed);
          while(r < nrows){
            assert !indexes.isEmpty();
            int i = rand.nextInt() % indexes.size();
            Key k = c._chunks[indexes.get(i)];
            r += DKV.get(k)._max/row_size;
            myKeys.add(k);
            int last = indexes.size()-1;
            indexes.set(i, indexes.get(last));
            indexes.remove(last);
          }
          chunkHistogram[keys.length][c._c] = Math.min(r,nrows);
        }
      }
    }

    int totalRows = 0; // size of local DataAdapter
    for(int i = 0; i < nclasses;++i)
      totalRows += chunkHistogram[keys.length][i];
    final DataAdapter dapt = new DataAdapter(ary, _drf._rfmodel, modelDataMap,
                                            totalRows,
                                            ValueArray.getChunkIndex(keys[0]),
                                            _drf._seed,
                                            _drf._binLimit,
                                            _drf._classWt);

    // vector keeping track of indexes of individual classes so that we can read data in parallel
    final int [] startRows = new int[nclasses];

    dapt.initIntervals(nclasses);
    for(int i = 1; i < nclasses; ++i){
      startRows[i] = startRows[i-1] + chunkHistogram[keys.length][i-1];
      dapt.setIntervalStart(i, startRows[i]);
    }
    // cols that do not need binning
    int [] rawCols = new int[_drf._rfmodel._va._cols.length];
    // cols that will be binned
    int [] binCols = new int[_drf._rfmodel._va._cols.length];
    int b = 0;
    int r = 0;

    //for(int i = 0; i < ary.numCols(); ++i){
    //  if(Arrays.binarySearch(_drf._ignores, i) < 0){
    //   // if(dapt.binColumn(i)) binCols[b++] = i;
    //   // else rawCols[r++] = i;
    //  }
    //}
    rawCols = Arrays.copyOf(rawCols, r);
    binCols = Arrays.copyOf(binCols, b);
    int [] rawMins = new int[r];
    for(int i = 0; i < rawCols.length; ++i)
      rawMins[i] = (int)ary._cols[rawCols[i]]._min;
    ArrayList<DataInhale> inhaleJobs = new ArrayList<DataInhale>();
    for (int i = 0; i < myKeys.size(); ++i) {
      Key k = myKeys.get(i);

      DataInhale job = new DataInhale(ary,
                                      dapt, _drf._rfmodel,
          startRows.clone(),
          k,
          binCols.length > 0,
          i < keys.length ? unbalancedClasses : null,
          binCols, rawCols, rawMins);

      inhaleJobs.add(job);
      if(i < keys.length) // local (majority class) chunk
        for(int j = 0; j < nclasses; ++j){
          if(unbalancedClasses == null || !unbalancedClasses[j])
            startRows[j] += chunkHistogram[i][j];
      } else { // chunk containing only single unbalanced class
        // find the unbalanced class
        int c = 0;
        for(;c < (_drf._uClasses.length-1) && i - _drf._uClasses[c]._chunks.length >= 0; ++c);
        startRows[c] += DKV.get(myKeys.get(i))._max/rpc;
      }
    }
    ForkJoinTask.invokeAll(inhaleJobs);

    // compute the binning
    if(binCols.length > 0){
      ArrayList<RecursiveAction> binningJobs = new ArrayList<RecursiveAction>();
      for(final int col : binCols) {
        binningJobs.add(new RecursiveAction() {
          @Override protected void compute(){
            dapt.computeBins(col);
          }
        });
      }
      ForkJoinTask.invokeAll(binningJobs);

      // Now do the inhale jobs again, this time reading all the values.
      ArrayList<DataInhale> inhaleJobs2 = new ArrayList<DataInhale>();
      for(DataInhale job: inhaleJobs) {
        inhaleJobs2.add(new DataInhale(job,_drf._rfmodel));
      }
      ForkJoinTask.invokeAll(inhaleJobs2);
    }

    return dapt;
  }
}
