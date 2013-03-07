package water;

import java.util.HashMap;
import water.DTask;

public class TypeMap {

  static private final String[] CLAZZES = {
    "hex.ConfusionMatrix",
    "hex.DGLM$GLMParams",
    "hex.DGLM$GLMValidation",
    "hex.DGLM$GramMatrixFunc",
    "hex.DGLM$Gram",
    "hex.DLSM$ADMMSolver",
    "hex.DLSM$GeneralizedGradientSolver",
    "hex.LinearRegression$CalcRegressionTask",
    "hex.LinearRegression$CalcSquareErrorsTasks",
    "hex.LinearRegression$CalcSumsTask",
    "hex.NewRowVecTask",
    "hex.NewRowVecTask$DataFrame",
    "hex.RowVecTask$Sampling",
    "water.H2ONode",
    "water.HeartBeat",
    "water.Jobs$1",
    "water.Jobs$Job",
    "water.Jobs$Progress$1",
    "water.KVTest$Atomic2",
    "water.KVTest$ByteHisto",
    "water.KVTest$RemoteBitSet",
    "water.Key",
    "water.TaskGetKey",
    "water.TaskPutKey",
    "water.TaskInvalidateKey",
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
