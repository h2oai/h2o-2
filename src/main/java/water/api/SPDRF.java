package water.api;

import hex.speedrf.*;
import hex.speedrf.DRF.DRFJob;
import hex.speedrf.Tree.StatType;

import java.util.*;

import water.*;
import water.fvec.Frame;
import water.util.RString;

import com.google.common.primitives.Ints;
import com.google.gson.JsonObject;

public class SPDRF extends Job.ModelJob{


//  protected final Key _dataKey = source._key;
//  protected final H2OHexKey         _dataKey    = new H2OHexKey(DATA_KEY);
//  protected final HexKeyClassCol    _classCol   = new HexKeyClassCol(CLASS, _dataKey);
//  public final Key _classCol = response._key;

  @API(help = "Number of trees", filter = Default.class, json = true, lmin = 1, lmax = Integer.MAX_VALUE)
  public final int _numTrees   = 50;
  @API(help = "Features", filter = Default.class, json = true, lmin = 1, lmax = Integer.MAX_VALUE)
  public final int _features = 1;
  @API(help = "Depth", filter = Default.class, json = true, lmin = 0, lmax = Integer.MAX_VALUE)
  public final int _depth = 20;

//  protected final EnumArgument<StatType> _statType = new EnumArgument<Tree.StatType>(STAT_TYPE, StatType.ENTROPY);
//  protected final HexColumnSelect   _ignore     = new RFColumnSelect(IGNORE, _dataKey, _classCol);
//  protected final H2OCategoryWeights _weights   = new H2OCategoryWeights(WEIGHTS, source._key, _classCol, 1);
//  protected final EnumArgument<Sampling.Strategy> _samplingStrategy = new EnumArgument<Sampling.Strategy>(SAMPLING_STRATEGY, Sampling.Strategy.RANDOM, true);
//  protected final H2OCategoryStrata               _strataSamples    = new H2OCategoryStrata(STRATA_SAMPLES, _dataKey, _classCol, 67);

  @API(help = "sample", filter = Default.class, json  = true, lmin = 1, lmax = 100)
  public final int _sample = 67;

  @API(help = "OOBEE", filter = Default.class, json = true)
  public final boolean _oobee = true;

  public final Key _modelKey = dest();
//  protected final H2OKey            _modelKey   = new H2OKey(MODEL_KEY, false);
  /* Advanced settings */
  @API(help = "bin limit", filter = Default.class, json = true, lmin = 0, lmax = 65534)
  public final int _binLimit = 1024;

  @API(help = "seed", filter = Default.class, json = true)
  public final long _seed       = (long) 1728318273;

  @API(help = "Parallel", filter = Default.class, json = true)
  public final boolean               _parallel   = true;
  @API(help = "split limit")
  public final int               _exclusiveSplitLimit = 0;

  @API(help = "iterative cm")
  public final boolean             _iterativeCM         = true;

  @API(help = "use non local data")
 public final boolean              _useNonLocalData     = true;

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='RF.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", DATA_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public SPDRF() {
//    _sample._hideInQuery = false; //default value for sampling strategy
//    _strataSamples._hideInQuery = true;
  }

//  @Override protected void queryArgumentValueSet(Argument arg, Properties inputArgs) {
//    if (arg == _samplingStrategy) {
////      _sample._hideInQuery = true; //_strataSamples._hideInQuery = true;
//      switch (_samplingStrategy.value()) {
////        case RANDOM                : _sample._hideInQuery = false; break;
////        case STRATIFIED_LOCAL      : _strataSamples._hideInQuery = false; break;
//      }
//    }
////    if( arg == _ignore ) {
////      int[] ii = _ignore.value();
////      if( ii != null && ii.length >= _dataKey.value()._cols.length )
////        throw new IllegalArgumentException("Cannot ignore all columns");
//    }
////  }

  /** Fires the random forest computation.
   */
  @Override public Response serve() {
    Frame fr = source;
    int classCol = fr.find(response); //.value();
    int ntree = _numTrees; //.value();

//    // invert ignores into accepted columns
//    BitSet bs = new BitSet();
//    bs.set(0,fr.vecs().length);
//    bs.clear(classCol);         // Not training on the class/response column
//    for( int i : _ignore.value() ) bs.clear(i);
//    int cols[] = new int[bs.cardinality()+1];
//    int idx=0;
//    for( int i=bs.nextSetBit(0); i >= 0; i=bs.nextSetBit(i+1))
//      cols[idx++] = i;
//    cols[idx++] = classCol;     // Class column last
//    assert idx==cols.length;
//    int[] cols = _ignore.value();

    Key dataKey = fr._key;
    Key modelKey = _modelKey !=null ? _modelKey : RFModel.makeKey();
    Lockable.delete(modelKey); // Remove any prior model first
//    for (int i = 0; i <= ntree; ++i) {
//      UKV.remove(ConfusionTask.keyForCM(modelKey,i,dataKey,classCol,true));
//      UKV.remove(ConfusionTask.keyForCM(modelKey,i,dataKey,classCol,false));
//    }

    int features            = _features;
    int exclusiveSplitLimit = _exclusiveSplitLimit;
//    int[]   ssamples        = _strataSamples.value();
//    float[] strataSamples   = new float[ssamples.length];
//    for(int i = 0; i<ssamples.length; i++) strataSamples[i] = ssamples[i]/100.0f;

    float[] samples = new float[(int) (response.max() - response.min() + 1)];
    for(int i = 0; i < samples.length; ++i) samples[i] = (float)67.0;
    double[] weigts = new double[(int) (response.max() - response.min() + 1)];
    for (int i = 0; i < weigts.length; ++i) weigts[i] = 1.0;
    try {
      // Async launch DRF
      DRFJob drfJob = DRF.execute(
              modelKey,
              cols,
              fr,
              fr.vecs()[fr.find(fr._names[classCol])],
              ntree,
              _depth,
              _binLimit,
              StatType.ENTROPY,
//              _statType.value(),
              _seed,
              _parallel,
              weigts,
              features,
              Sampling.Strategy.RANDOM,
              _sample / 100.0f,
              samples,
              0, /* verbose level is minimal here */
              exclusiveSplitLimit,
              _useNonLocalData
      );
      // Collect parameters required for validation.
      JsonObject response = new JsonObject();
      response.addProperty(DATA_KEY, dataKey.toString());
      response.addProperty(MODEL_KEY, drfJob.dest().toString());
      response.addProperty(DEST_KEY, drfJob.dest().toString());
      response.addProperty(NUM_TREES, ntree);
      response.addProperty(CLASS, classCol);
      Response r = SPDRFView.redirect(response, drfJob.self(), drfJob.dest(), dataKey, ntree, classCol, "1.0,1.0", _oobee, _iterativeCM);
      r.setBuilder(DEST_KEY, new KeyElementBuilder());
      return r;
    } catch( IllegalArgumentException e ) {
      return Response.error("Incorrect input data: "+e.getMessage());
    }

  }

  // By default ignore all constants columns and warn about "bad" columns, i.e., columns with
  // many NAs (>25% of NAs)
//  class RFColumnSelect extends HexNonConstantColumnSelect {
//
//    public RFColumnSelect(String name, H2OHexKey key, H2OHexKeyCol classCol) {
//      super(name, key, classCol);
//    }
//
//    @Override protected int[] defaultValue() {
//      ValueArray va = _key.value();
//      int [] res = new int[va._cols.length];
//      int selected = 0;
//      for(int i = 0; i < va._cols.length; ++i)
//        if(shouldIgnore(i,va._cols[i]))
//          res[selected++] = i;
//        else if((1.0 - (double)va._cols[i]._n/va._numrows) >= _maxNAsRatio) {
//          //res[selected++] = i;
//          int val = 0;
//          if(_badColumns.get() != null) val = _badColumns.get();
//          _badColumns.set(val+1);
//        }
//
//      return Arrays.copyOfRange(res,0,selected);
//    }
//
//    @Override protected int[] parse(String input) throws IllegalArgumentException {
//      int[] result = super.parse(input);
//      return Ints.concat(result, defaultValue());
//    }
//
//    @Override public String queryComment() {
//      TreeSet<String> ignoredCols = _constantColumns.get();
//      StringBuilder sb = new StringBuilder();
//      if(_badColumns.get() != null && _badColumns.get() > 0)
//        sb.append("<b> There are ").append(_badColumns.get()).append(" columns with more than ").append(_maxNAsRatio*100).append("% of NAs.<br/>");
//      if (ignoredCols!=null && !ignoredCols.isEmpty())
//        sb.append("Ignoring ").append(_constantColumns.get().size()).append(" constant columns</b>: ").append(ignoredCols.toString());
//      if (sb.length()>0) sb.insert(0, "<div class='alert'>").append("</div>");
//      return sb.toString();
//    }
//  }
}
