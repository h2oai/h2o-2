package water;

import java.util.HashMap;
import water.DTask;

public class TypeMap {

  static private final String[] CLAZZES = {
    "hex.ConfusionMatrix",
    "hex.DGLM$GLMParams",
    "hex.DGLM$GLMValidation",
    "hex.DGLM$GLMValidationFunc",
    "hex.DGLM$GramMatrixFunc",
    "hex.DGLM$Gram",
    "hex.DLSM$ADMMSolver",
    "hex.DLSM$GeneralizedGradientSolver",
    "hex.KMeans$Lloyds",
    "hex.KMeans$Sampler",
    "hex.KMeans$Sqr",
    "hex.LinearRegression$CalcRegressionTask",
    "hex.LinearRegression$CalcSquareErrorsTasks",
    "hex.LinearRegression$CalcSumsTask",
    "hex.NewRowVecTask",
    "hex.NewRowVecTask$DataFrame",
    "hex.RowVecTask$Sampling",
    "hex.rf.DRF",
    "hex.rf.RFModel",
    "water.exec.Helpers$1",
    "water.exec.InPlaceColSwap$ColSwapTask",
    "water.H2ONode",
    "water.HeartBeat",
    "water.Jobs$1",
    "water.Jobs$2",
    "water.Jobs$3",
    "water.Jobs$Job",
    "water.Jobs$Progress$1",
    "hex.rf.Tree$1",
    "hex.rf.MinorityClasses$ClassExtractTask",
    "hex.rf.MinorityClasses$CountMClassRowsTask",
    "hex.rf.MinorityClasses$HistogramTask",
    "hex.rf.MinorityClasses$ReplicationTask",
    "hex.rf.MinorityClasses$UnbalancedClass",
    "water.H2ONode",
    "water.HeartBeat",
    "water.KVTest$Atomic2",
    "water.KVTest$ByteHisto",
    "water.KVTest$RemoteBitSet",
    "water.Key",
    "water.TaskGetKey",
    "water.TaskInvalidateKey",
    "water.TaskPutKey",
    "water.Value",
    "water.ValueArray",
    "water.ValueArray$Column",
    "water.exec.IifOperatorScalar23",
    "water.exec.LeftEq",
    "water.parser.DParseTask",
    "water.parser.DParseTask$AtomicUnion",
    "water.parser.Enum",
    "water.util.JStackCollectorTask",
  };
  static private final HashMap<String,Integer> MAP = new HashMap();
  static {
    for( int i=0; i<CLAZZES.length; i++ )
      MAP.put(CLAZZES[i],i);
  }

  static private final Freezable[] GOLD = new Freezable[CLAZZES.length];

  static public Freezable getType(int id) {
    Freezable f = GOLD[id];
    if( f == null ) {
      try { GOLD[id] = f = (Freezable) Class.forName(CLAZZES[id]).newInstance(); }
      catch( Exception e ) { throw new Error(e); }
    }
    return f.newInstance();
  }

  static public int getId(Freezable f) {
    Integer I = MAP.get(f.getClass().getName());
    assert I != null : "TypeMap missing "+f.getClass().getName();
    return I;
  }
}
