package water.api;

import hex.gbm.DRF;
import water.DKV;
import water.Key;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.RString;

public class DRF2 extends Request {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Build a model using distributed Random Forest";

  @API(help="Frame to build model from.")
  final FrameKey data_key = new FrameKey("data_key");

  @API(help="Response variable that is being learned.")
  protected final FrameClassVec class_vec = new FrameClassVec("class_vec", data_key);

  @API(help="Number of trees to build.")
  protected final Int ntrees = new Int("ntrees",10,0,1000000);

  @API(help="Number of split features used for tree building.  The default value is sqrt(#columns).")
  protected final Int features = new Int("features", null, 1, Integer.MAX_VALUE);

  @API(help="Max tree depth.")
  protected final Int depth = new Int("depth",Integer.MAX_VALUE,0,Integer.MAX_VALUE);

  @API(help="Select columns to model on.")
  protected final FrameNonClassVecSelect vecs  = new FrameNonClassVecSelect("vecs",data_key,class_vec);

  @API(help="Sampling rate during tree building.")
  protected final Real sample_rate = new Real("sample_rate", 0.67, 0.0, 1.0,"");

  @API(help="Psuedo-random number generator seed.")
  protected final LongInt seed = new LongInt("seed",0xae44a87f9edf1cbL,"High order bits make better seeds");

//  protected final Bool              _oobee      = new Bool(OOBEE,true,"Out of bag error");
//  protected final H2OKey            _modelKey   = new H2OKey(MODEL_KEY, false);

  /** Return the query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='DRF2.query?data_key=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override public Response serve() {
    Frame fr = DKV.get(data_key.value()).get();
    if( fr == null ) return RequestServer._http404.serve();
    // Build a frame with the selected Vecs
    Frame fr2 = new Frame(new String[0],new Vec[0]);
    int[] idxs = vecs.value();
    for( int idx : idxs )       // The selected frame columns
      fr2.add(fr._names[idx],fr._vecs[idx]);
    // Add the class-vec last
    fr2.add(fr._names[class_vec._colIdx.get()],class_vec.value());
    int mtrys = features.value()==null
      ? (int)(Math.sqrt(idxs.length)+0.5)
      : features.value();

    DRF drf = DRF.start(DRF.makeKey(),
                        fr2,
                        depth.value(),
                        ntrees.value(),
                        mtrys,
                        sample_rate.value(),
                        seed.value());

    drf.get();                  // Block for result

    return new Response(Response.Status.done, this, -1, -1, null);
  }

//  // By default ignore all constants columns and warn about "bad" columns,
//  // i.e., columns with many NAs (>25% of NAs).
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
//            //res[selected++] = i;
//            int val = 0;
//            if(_badColumns.get() != null) val = _badColumns.get();
//            _badColumns.set(val+1);
//          }
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
//      if(_badColumns.get() != null && _badColumns.get() > 0)
//        return "<div class='alert'><b> There are " + _badColumns.get() + " columns with more than " + _maxNAsRatio*100 + "% of NAs.<br/>\nIgnoring " + _constantColumns.get().size() + " constant columns</b>: " + ignoredCols.toString() +"</div>";
//      return super.queryComment();
//    }
//  }
}
