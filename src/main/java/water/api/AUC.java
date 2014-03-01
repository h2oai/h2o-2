package water.api;

import water.MRTask2;
import water.Request2;
import water.UKV;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;

public class AUC extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "", required = true, filter = Default.class)
  public Frame actual;

  @API(help="Column of the actual results (will display vertically)", required=true, filter=actualVecSelect.class)
  public Vec vactual;
  class actualVecSelect extends VecClassSelect { actualVecSelect() { super("actual"); } }

  @API(help = "", required = true, filter = Default.class)
  public Frame predict;

  @API(help="Column of the predicted results (will display horizontally)", required=true, filter=predictVecSelect.class)
  public Vec vpredict;
  class predictVecSelect extends VecClassSelect { predictVecSelect() { super("predict"); } }

  @API(help="domain of the actual response")
  String [] actual_domain;
  @API(help="AUC")
  public double auc;

  @Override public Response serve() {
    Vec va = null,vp = null;
    // Input handling
    if( vactual==null || vpredict==null )
      throw new IllegalArgumentException("Missing actual or predict!");
    if (vactual.length() != vpredict.length())
      throw new IllegalArgumentException("Both arguments must have the same length!");
    if (!vactual.isInt())
      throw new IllegalArgumentException("Actual column must be integer class labels!");
    if (vpredict.isInt())
      throw new IllegalArgumentException("Predicted column must be a floating point probability!");

    try {
      va = vactual .toEnum(); // always returns TransfVec
      actual_domain = va._domain;
      vp = vpredict;
      // The vectors are from different groups => align them, but properly delete it after computation
      if (!va.group().equals(vp.group())) {
        vp = va.align(vp);
      }
      auc = new AUCTask(100).doAll(va,vp).auc();
      return Response.done(this);
    } catch( Throwable t ) {
      return Response.error(t);
    } finally {       // Delete adaptation vectors
      if (va!=null) UKV.remove(va._key);
    }
  }

  // Compute the AUC
  private static class AUCTask extends MRTask2<AUCTask> {
    /* @IN */ final int _bins;
    /* @OUT AUC */ public double auc() { return _auc; }
    /* Helper */ private double _auc;
    AUCTask(int bins) { _bins = bins;  }
    @Override public void map( Chunk ca, Chunk cp ) {
      int len = Math.min(ca._len,cp._len); // handle different lenghts, but the vectors should have been rejected already
      for( int i=0; i < _bins; i++ ) {
        _auc = 0;
      }
    }

    @Override public void reduce( AUCTask other ) {
      assert(_auc != Double.NaN && other._auc != Double.NaN);
      _auc += other._auc;
    }
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    DocGen.HTML.section(sb,"AUC");
    DocGen.HTML.arrayHead(sb);
    sb.append("<tr class='warning'><td>" + auc + "</td></tr>"); // Row header
    DocGen.HTML.arrayTail(sb);
    return true;
  }

  public double toASCII( StringBuilder sb ) {
    sb.append("AUC: " + auc);
    return auc;
  }
}
