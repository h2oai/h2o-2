package hex;

import hex.DGLM.Family;
import hex.DGLM.GLMModel;
import hex.DGLM.GLMParams;
import hex.DLSM.ADMMSolver;
import hex.DLSM.LSMSolver;

import java.util.Arrays;

import org.junit.Test;

import water.*;

// Test grid-search over GLM args
public class GLMGridTest extends TestUtil {

  static final double [] thresholds = new double [] {0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9};
  private static GLMModel compute_glm_score( ValueArray va, int[] cols, GLMParams glmp, LSMSolver lsms, double thresh, String msg ) {
    // Binomial (logistic) GLM solver
    glmp._family = Family.binomial;
    glmp._link = glmp._family.defaultLink; // logit
    //glmp._familyArgs = glmp._f.defaultArgs; // no case/weight.  default 0.5 thresh
    glmp._betaEps = 0.000001;
    glmp._maxIter = 100;

    System.out.print(msg);

    // Solve it!
    GLMModel m = DGLM.buildModel(DGLM.getData(va, cols, null, true), lsms, glmp);
    if( m._warnings != null )
      for( String s : m._warnings )
        System.err.println(s);

    // Validate / compute results
    if( m.isSolved() ) {
      m.validateOn(va,null,thresholds);
      long[][] arr = m._vals[0].bestCM()._arr;
      System.out.println(", score="+score(m)+Arrays.deepToString(arr));
    }
    return m;
  }

  // An array from 0 to length with increasing int columns,
  // skipping 'skip' and 'class_col'  Add class_col at the end.
  private static void cols( int[] cols, int class_col, int skip ) {
    int i=0, j=0;
    while( j<cols.length-1 ) {
      if( i != class_col && i != skip )
        cols[j++] = i;
      i++;
    }
    cols[j] = class_col;
  }

  // Which model is better?  Weeny optimization function seeks to minimize the
  // max error rate per-class.
  private static boolean better( GLMModel best, GLMModel x ) {
    if( best == null || !best.isSolved() ) return true;
    if( x    == null || !x.   isSolved() ) return false;
    return score(best) > score(x);
  }

  // Max of error-rates per-row.  Lower score is better (lower max-error)
  private static double score( GLMModel m ) {
    long[][] arr = m._vals[0].bestCM()._arr;
    double err0 = arr[0][1]/(double)(arr[0][0]+arr[0][1]);
    double err1 = arr[1][0]/(double)(arr[1][0]+arr[1][1]);
    return Math.max(err0,err1);
  }

  // Minimize max-errors of prostate or hhp
  @Test public void test_PROSTATE_CSV() {
    Key k1=null;
    try {
      // Load dataset
      //k1 = loadAndParseKey("h.hex","smalldata/logreg/prostate.csv"); final int class_col = 1;
      k1 = loadAndParseKey("h.hex","smalldata/hhp_107_01.data.gz"); final int class_col = 106;
      ValueArray va = ValueArray.value(DKV.get(k1));
      // Default normalization solver
      LSMSolver lsms = new ADMMSolver(1e-5,0.5);
      // Binomial (logistic) GLM solver
      GLMParams glmp = new GLMParams(Family.binomial);

      // Initial columns: all, with the class moved to the end
      int[] cols = new int[va._cols.length];
      cols(cols,class_col,-1);

      GLMModel m = compute_glm_score(va,cols,glmp,lsms,0.5,"initial");
      GLMModel best = m.isSolved() ? m : null;

      // Try with 1 column removed
      cols = new int[va._cols.length-1];
      cols(cols,class_col,-1);
      for( int skip=0; skip<va._cols.length; skip++ ) {
        if( skip != class_col ) {
          cols(cols,class_col,skip);
          m = compute_glm_score(va,cols,glmp,lsms,0.5,"ignoring col "+skip);
          if( better(best,m) ) {
            best = m;
            System.out.println("Picking better model");
          }
        }
      }

      // Pick with 'IDX' removed
      cols(cols,class_col,0);
      compute_glm_score(va,cols,glmp,lsms,0.5,"ignoring col "+0);

      // Schmoo over threshold
      for( double t = 0.0; t<=1.0; t += 0.1 )
        compute_glm_score(va,cols,glmp,lsms,t,"thresh="+t);

    } finally {
      UKV.remove(k1);
    }
  }
}
