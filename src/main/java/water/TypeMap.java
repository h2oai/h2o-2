package water;

import java.util.HashMap;
import water.DTask;

public class TypeMap {

  static private final String[] CLAZZES = {
    "hex.ConfusionMatrix",
    "hex.DGLM$GLMParams",
    "hex.DGLM$GLMValidation",
    "hex.DGLM$GLMValidationFunc",
    "hex.DGLM$Gram",
    "hex.DGLM$GramMatrixFunc",
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
    "hex.rf.Confusion",
    "hex.rf.DRF",
    "hex.rf.MinorityClasses$ClassExtractTask",
    "hex.rf.MinorityClasses$CountMClassRowsTask",
    "hex.rf.MinorityClasses$HistogramTask",
    "hex.rf.MinorityClasses$ReplicationTask",
    "hex.rf.MinorityClasses$UnbalancedClass",
    "hex.rf.RFModel",
    "hex.rf.Tree$1",
    "water.AppendKey",
    "water.H2ONode",
    "water.H2ONode",
    "water.HeartBeat",
    "water.HeartBeat",
    "water.Jobs$1",
    "water.Jobs$2",
    "water.Jobs$3",
    "water.Jobs$Job",
    "water.Jobs$Progress$1",
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
    "water.exec.AddOperator",
    "water.exec.AndOperator",
    "water.exec.BooleanVectorFilter",
    "water.exec.DeepSingleColumnAssignment",
    "water.exec.DivOperator",
    "water.exec.EqOperator",
    "water.exec.GreaterOperator",
    "water.exec.GreaterOrEqOperator",
    "water.exec.Helpers$1",
    "water.exec.Helpers$ScallarCollector",
    "water.exec.Helpers$SigmaCalc",
    "water.exec.IifOperator",
    "water.exec.IifOperatorScalar2",
    "water.exec.IifOperatorScalar23",
    "water.exec.IifOperatorScalar3",
    "water.exec.InPlaceColSwap$ColSwapTask",
    "water.exec.LeftAdd",
    "water.exec.LeftAnd",
    "water.exec.LeftDiv",
    "water.exec.LeftEq",
    "water.exec.LeftEq",
    "water.exec.LeftGreater",
    "water.exec.LeftGreaterOrEq",
    "water.exec.LeftLess",
    "water.exec.LeftLessOrEq",
    "water.exec.LeftMod",
    "water.exec.LeftMul",
    "water.exec.LeftNeq",
    "water.exec.LeftOr",
    "water.exec.LeftSub",
    "water.exec.LessOperator",
    "water.exec.LessOrEqOperator",
    "water.exec.Log$MRLog",
    "water.exec.MakeEnum$GetEnumTask",
    "water.exec.MakeEnum$PackToEnumTask",
    "water.exec.Max$MRMax",
    "water.exec.Mean$MRMean",
    "water.exec.Min$MRMin",
    "water.exec.ModOperator",
    "water.exec.MulOperator",
    "water.exec.NeqOperator",
    "water.exec.OrOperator",
    "water.exec.RandBitVect$RandVectBuilder",
    "water.exec.RightAnd",
    "water.exec.RightDiv",
    "water.exec.RightMod",
    "water.exec.RightOr",
    "water.exec.RightSub",
    "water.exec.SliceFilter",
    "water.exec.SubOperator",
    "water.exec.Sum$MRSum",
    "water.exec.UnaryMinus",
    "water.parser.DParseTask",
    "water.parser.DParseTask$AtomicUnion",
    "water.parser.Enum",
    "water.util.FileIntegrityChecker",
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
