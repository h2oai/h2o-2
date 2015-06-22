/**
 * Created by prateem on 3/5/15.
 */

package hex;

import hex.glm.GLMModel;
import org.apache.commons.lang.ArrayUtils;
import water.Iced;
import water.Key;
import water.MRTask2;
import water.MemoryManager;
import water.api.Request;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class EvalModelAttrib {
  @API(
    help = "model",
    required = true,
    filter = Request.Default.class)
  public GLMModel model;

  @API(
    help = "stack",
    required = true,
    filter = Request.Default.class)
  public Frame stackFrame;

  @API(
    help = "base coefficient names",
    required = true,
    filter = Request.Default.class)
  public String baseNames;

  @API(
    help = "marketing coefficient names",
    required = true,
    filter = Request.Default.class)
  public String marketingNames;

  public static class ModelAtribTask extends MRTask2<ModelAtribTask> {
    final double [] _beta;
    int [] _base;
    int [] _marketing;
    double [] _lift;
    long _nobs;

    public ModelAtribTask(double [] beta, int [] base, int [] marketing) {
      _beta = beta;
      _base = base;
      _marketing = marketing;
    }
    public void map(Chunk [] chunks) {
      _lift = MemoryManager.malloc8d(_marketing.length);
      double [] mps = MemoryManager.malloc8d(_marketing.length);
      for(int r = 0; r < chunks[0]._len; ++r ) {
        for(Chunk c:chunks) if(c.isNA0(r)) continue;
        ++_nobs;
        double base = _beta[_beta.length-1]; // intercept
        for (int i:_base)
          base += _beta[i] * chunks[i].at0(r);
        double full = base;
        for(int i = 0; i < _marketing.length; ++i) {
          int idx = _marketing[i];
          double d = _beta[idx]*chunks[idx].at0(r);
          full += d;
          mps[i] = base + d;
        }
        double fullP = (Math.exp(-full) + 1.0); // fullP inverse
        double baseP = 1.0/(Math.exp(-base) + 1);
        for(int i = 0; i < _marketing.length; ++i)
          _lift[i] += (1.0/(Math.exp(-mps[i]) + 1) - baseP)*fullP;
      }
    }
    @Override public void postGlobal(){
      double d = 1.0/_nobs;
      for(int i = 0; i < _lift.length; ++i)
        _lift[i] *= d;
    }
    public void reduce(ModelAtribTask mat) {
      _nobs += mat._nobs;
      for(int i = 0; i < _lift.length; ++i)
        _lift[i] += mat._lift[i];
    }
  }
  public static class LiftResult extends Iced {
    public final long nobs;
    public final String [] marketingVars;
    public final double [] liftValues;
    public LiftResult(long nobs, String [] mVars, double [] lVals){
      this.nobs = nobs;
      this.marketingVars = mVars;
      this.liftValues = lVals;
    }
  }
  public static double [] scoreModelAttrib(
    GLMModel model,
    Frame stackFrame,
    String baseNames,
    String marketingNames) {
    /* Convert the variable names string into list of names. */
    List<String> baseNamesList = Arrays.asList(baseNames.trim().split("\\s*,\\s*"));
    List<String> marketingNamesList =
      Arrays.asList(marketingNames.trim().split("\\s*,\\s*"));
    HashSet<String> mVars = new HashSet<String>(marketingNamesList);
    HashSet<String> bVars = new HashSet<String>(baseNamesList);
    bVars.remove("Intercept"); // remove intercept if present
    int [] mIds = new int[mVars.size()];
    int [] bIds = new int[bVars.size()];
    String [] coefNames = model.coefficients_names;
    String [] mVarNames = new String[mVars.size()];
    int j = 0, k = 0;
    for(int i = 0; i < coefNames.length; ++i)
      if(mVars.contains(coefNames[i])) {
        mIds[j] = i;
        mVarNames[j] = coefNames[i];
        ++j;
      } else if(bVars.contains(coefNames[i]))
        bIds[k++] = i;
    assert j == mIds.length;
    assert bIds.length == k;
    Frame [] frs = model.adapt(stackFrame,true,true);
    double [] lift = new ModelAtribTask(model.beta(),bIds,mIds).doAll(frs[0])._lift;
    frs[1].delete();
    // may need to permute the marketing vars back
    double [] res = new double[lift.length];
    j = 0;
    for(String mVar: marketingNamesList) {
      for(int i = 0; i < mVarNames.length; ++i) {
        if(mVar.equals(mVarNames[i])) {
          res[j++] = lift[i];
          break;
        }
      }
    }
    return res;
  }
}
