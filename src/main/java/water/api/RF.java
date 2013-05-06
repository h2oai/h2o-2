package water.api;

import hex.rf.*;
import hex.rf.DRF.DRFJob;
import hex.rf.Tree.StatType;

import java.util.*;

import water.*;
import water.util.RString;

import com.google.common.primitives.Ints;
import com.google.gson.JsonObject;

public class RF extends Request {

  protected final H2OHexKey         _dataKey    = new H2OHexKey(DATA_KEY);
  protected final HexKeyClassCol    _classCol   = new HexKeyClassCol(CLASS, _dataKey);
  protected final Int               _numTrees   = new Int(NUM_TREES,50,0,Integer.MAX_VALUE);
  protected final Int               _features   = new Int(FEATURES, null, 1, Integer.MAX_VALUE);
  protected final Int               _depth      = new Int(DEPTH,Integer.MAX_VALUE,0,Integer.MAX_VALUE);
  protected final EnumArgument<StatType> _statType = new EnumArgument<Tree.StatType>(STAT_TYPE, StatType.ENTROPY);
  protected final HexColumnSelect   _ignore     = new RFColumnSelect(IGNORE, _dataKey, _classCol);
  protected final H2OCategoryWeights _weights   = new H2OCategoryWeights(WEIGHTS, _dataKey, _classCol, 1);
  protected final EnumArgument<Sampling.Strategy> _samplingStrategy = new EnumArgument<Sampling.Strategy>(SAMPLING_STRATEGY, Sampling.Strategy.RANDOM, true);
  protected final H2OCategoryStrata               _strataSamples    = new H2OCategoryStrata(STRATA_SAMPLES, _dataKey, _classCol, 67);
  protected final Int               _sample     = new Int(SAMPLE, 67, 1, 100);
  protected final Bool              _oobee      = new Bool(OOBEE,true,"Out of bag error");
  protected final H2OKey            _modelKey   = new H2OKey(MODEL_KEY, RFModel.makeKey());
  /* Advanced settings */
  protected final Int               _binLimit   = new Int(BIN_LIMIT,1024, 0,65534);
  protected final LongInt           _seed       = new LongInt(SEED,0xae44a87f9edf1cbL,"High order bits make better seeds");
  protected final Bool              _parallel   = new Bool(PARALLEL,true,"Build trees in parallel");
  protected final Int               _exclusiveSplitLimit = new Int(EXCLUSIVE_SPLIT_LIMIT, null, 0, Integer.MAX_VALUE);
  protected final Bool               _iterativeCM        = new Bool(ITERATIVE_CM, true, "Compute confusion matrix on-the-fly");

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='RF.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", DATA_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public RF() {
    _sample._hideInQuery = false; //default value for sampling strategy
    _strataSamples._hideInQuery = true;
    // Request help
    help(this, "Build a model using Random Forest.");
    // Fields help
    help(_dataKey,  "Dataset.");
    help(_classCol, "The output classification (also known as " +
    		        "'response variable') that is being learned.");
    help(_numTrees, "Number of trees to generate.");
    help(_features, "Number of split features,");
    help(_depth,    "Maximal depth of a tree.");
    help(_oobee,    "Compute out-of-bag error estimation (OOBEE).");
    help(_modelKey, "Random forest model's key.");
    help(_binLimit, "Bin limit.");
  }

  @Override protected void queryArgumentValueSet(Argument arg, Properties inputArgs) {
    if (arg == _samplingStrategy) {
      _sample._hideInQuery = true; _strataSamples._hideInQuery = true;
      switch (_samplingStrategy.value()) {
      case RANDOM                : _sample._hideInQuery = false; break;
      case STRATIFIED_LOCAL      : _strataSamples._hideInQuery = false; break;
      }
    }
    if( arg == _ignore ) {
      int[] ii = _ignore.value();
      if( ii != null && ii.length >= _dataKey.value()._cols.length )
        throw new IllegalArgumentException("Cannot ignore all columns");
    }
  }

  /** Fires the random forest computation.
   */
  @Override public Response serve() {
    JsonObject response = new JsonObject();
    ValueArray ary = _dataKey.value();
    int classCol = _classCol.value();
    int ntree = _numTrees.value();

    // invert ignores into accepted columns
    BitSet bs = new BitSet();
    bs.set(0,ary._cols.length);
    bs.clear(classCol);         // Not training on the class/response column
    for( int i : _ignore.value() ) bs.clear(i);
    int cols[] = new int[bs.cardinality()+1];
    int idx=0;
    for( int i=bs.nextSetBit(0); i >= 0; i=bs.nextSetBit(i+1))
      cols[idx++] = i;
    cols[idx++] = classCol;     // Class column last
    assert idx==cols.length;

    Key dataKey = ary._key;
    Key modelKey = _modelKey.value();
    UKV.remove(modelKey);       // Remove any prior model first
    for (int i = 0; i < ntree; ++i) {
      UKV.remove(Confusion.keyFor(modelKey,i,dataKey,classCol,true));
      UKV.remove(Confusion.keyFor(modelKey,i,dataKey,classCol,false));
    }

    int features            = _features.value() == null ? -1 : _features.value();
    int exclusiveSplitLimit = _exclusiveSplitLimit.value() == null ? 0 : _exclusiveSplitLimit.value();
    int[]   ssamples        = _strataSamples.value();
    float[] strataSamples   = new float[ssamples.length];
    for(int i = 0; i<ssamples.length; i++) strataSamples[i] = ssamples[i]/100.0f;

    try {
      // Async launch DRF
      DRFJob drfJob = hex.rf.DRF.execute(
              modelKey,
              cols,
              ary,
              ntree,
              _depth.value(),
              _binLimit.value(),
              _statType.value(),
              _seed.value(),
              _parallel.value(),
              _weights.value(),
              features,
              _samplingStrategy.value(),
              _sample.value() / 100.0f,
              strataSamples,
              0, /* verbose level is minimal here */
              exclusiveSplitLimit
              );
      // Collect parameters required for validation.
      response.addProperty(DATA_KEY, dataKey.toString());
      response.addProperty(MODEL_KEY, modelKey.toString());
      response.addProperty(NUM_TREES, ntree);
      response.addProperty(CLASS, classCol);
      if (_weights.specified())
        response.addProperty(WEIGHTS, _weights.originalValue());
      if (_ignore.specified())
        response.addProperty(IGNORE, _ignore.originalValue());
      response.addProperty(OOBEE, _oobee.value());
      response.addProperty(ITERATIVE_CM, _iterativeCM.value());

      return Response.redirect(response, RFView.class, response);
    } catch (IllegalArgumentException e) {
      return Response.error("Incorrect input data: "+e.getMessage());
    }
  }

  // By default ignore all constants columns and warn about "bad" columns, i.e., columns with
  // many NAs (>25% of NAs)
  class RFColumnSelect extends HexNonConstantColumnSelect {

    public RFColumnSelect(String name, H2OHexKey key, H2OHexKeyCol classCol) {
      super(name, key, classCol);
    }

    @Override protected int[] defaultValue() {
      ValueArray va = _key.value();
      int [] res = new int[va._cols.length];
      int selected = 0;
      for(int i = 0; i < va._cols.length; ++i)
        if(shouldIgnore(i,va._cols[i]))
          res[selected++] = i;
        else if((1.0 - (double)va._cols[i]._n/va._numrows) >= _maxNAsRatio) {
            //res[selected++] = i;
            int val = 0;
            if(_badColumns.get() != null) val = _badColumns.get();
            _badColumns.set(val+1);
          }

      return Arrays.copyOfRange(res,0,selected);
    }

    @Override protected int[] parse(String input) throws IllegalArgumentException {
      int[] result = super.parse(input);
      return Ints.concat(result, defaultValue());
    }

    @Override public String queryComment() {
      TreeSet<String> ignoredCols = _constantColumns.get();
      if(_badColumns.get() != null && _badColumns.get() > 0)
        return "<div class='alert'><b> There are " + _badColumns.get() + " columns with more than " + _maxNAsRatio*100 + "% of NAs.<br/>\nIgnoring " + _constantColumns.get().size() + " constant columns</b>: " + ignoredCols.toString() +"</div>";
      return super.queryComment();
    }
  }
}
