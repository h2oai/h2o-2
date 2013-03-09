package water.api;

import hex.rf.*;

import java.util.Properties;
import java.util.BitSet;

import water.*;
import water.util.RString;

import com.google.gson.JsonObject;

public class RF extends Request {

  protected final H2OHexKey _dataKey = new H2OHexKey(DATA_KEY);
  protected final HexKeyClassCol _classCol = new HexKeyClassCol(CLASS, _dataKey);
  protected final Int _numTrees = new Int(NUM_TREES,50,0,Integer.MAX_VALUE);
  protected final Bool _gini = new Bool(GINI,false,"use gini statistic (otherwise entropy is used)");
  protected final H2OCategoryWeights _weights = new H2OCategoryWeights(WEIGHTS, _dataKey, _classCol, 1);
  protected final Bool _stratify = new Bool(STRATIFY,false,"Use Stratified sampling");
  protected final H2OCategoryStrata _strata = new H2OCategoryStrata(STRATA, _dataKey, _classCol, 1);
  protected final H2OKey _modelKey = new H2OKey(MODEL_KEY, RFModel.makeKey());
  protected final Bool _oobee = new Bool(OOBEE,true,"Out of bag errors");
  protected final Int _features = new Int(FEATURES, null, 1, Integer.MAX_VALUE);
  protected final HexColumnSelect _ignore = new HexNonClassColumnSelect(IGNORE, _dataKey, _classCol);
  protected final Int _sample = new Int(SAMPLE, 67, 1, 100);
  protected final Int _binLimit = new Int(BIN_LIMIT,1024, 0,65535);
  protected final Int _depth = new Int(DEPTH,Integer.MAX_VALUE,0,Integer.MAX_VALUE);
  protected final LongInt _seed = new LongInt(SEED,0xae44a87f9edf1cbL,"High order bits make better seeds");
  protected final Bool _parallel = new Bool(PARALLEL,true,"Build trees in parallel");
  protected final Int _exclusiveSplitLimit = new Int(EXCLUSIVE_SPLIT_LIMIT, null, 0, Integer.MAX_VALUE);

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='RF.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", DATA_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public RF() {
    _stratify.setRefreshOnChange();
    _requestHelp = "Build a model using Random Forest.";
    _classCol._requestHelp = "The output classification (also known as " +
    		"'response variable') that is being learned.";
  }

  @Override protected void queryArgumentValueSet(Argument arg, Properties inputArgs) {
    if (arg == _stratify) {
      if (_stratify.value()) {
        _oobee.disable("OOBEE is only meaningful if stratify is not specified.", inputArgs);
        _oobee.record()._value = false;
      } else {
        _strata.disable("Strata is only meaningful if stratify is on.", inputArgs);
      }
    }
    if( arg == _ignore ) {
      int[] ii = _ignore.value();
      if( ii != null && ii.length >= _dataKey.value()._cols.length-1 )
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

    int features = _features.value() == null ? -1 : _features.value();
    float sample = _sample.value() / 100.0f;
    int exclusiveSplitLimit = _exclusiveSplitLimit.value() == null ? 0 : _exclusiveSplitLimit.value();
    Tree.StatType statType = _gini.value() ? Tree.StatType.GINI : Tree.StatType.ENTROPY;

    try {
      // Async launch DRF
      hex.rf.DRF.execute(
              modelKey,         // Key to save the model under
              cols,             // Columns selected from the dataset
              ary,              // The dataset to train on
              ntree,
              _depth.value(),
              sample,
              _binLimit.value().shortValue(),
              statType,
              _seed.value(),
              _parallel.value(),
              _weights.value(),
              features,
              _stratify.value(),
              _strata.convertToMap(),
              0, /* verbose level is minimal here */
              exclusiveSplitLimit
              );
      response.addProperty(DATA_KEY, dataKey.toString());
      response.addProperty(MODEL_KEY, modelKey.toString());
      response.addProperty(NUM_TREES, ntree);
      response.addProperty(CLASS, classCol);
      if (_weights.specified())
        response.addProperty(WEIGHTS, _weights.originalValue());
      if (_ignore.specified())
        response.addProperty(IGNORE, _ignore.originalValue());
      response.addProperty(OOBEE, _oobee.value());

      return Response.redirect(response, RFView.class, response);
    } catch (IllegalArgumentException e) {
      return Response.error("Incorrect input data: "+e.getMessage());
    }
  }
}
