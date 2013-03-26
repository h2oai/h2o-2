package water;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class TypeMap {
  static public final short NULL = (short) -1;
  static public final short PRIM_B = 1;
  static public final short VALUE_ARRAY;

  // Run main below to update TODO add as build step
  static private final String[] CLAZZES = {
    " BAD",                     // 0: BAD
    "[B",                       // 1: Array of Bytes
    "hex.ConfusionMatrix",
    "hex.Covariance$COV_Task",
    "hex.DGLM$GLMModel",
    "hex.DGLM$GLMParams",
    "hex.DGLM$GLMValidation",
    "hex.DGLM$GLMValidationFunc",
    "hex.DGLM$Gram",
    "hex.DGLM$GramMatrixFunc",
    "hex.DLSM$ADMMSolver",
    "hex.DLSM$GeneralizedGradientSolver",
    "hex.DLSM$LSMSolver",
    "hex.GLMGrid$1",
    "hex.GLMGrid$2",
    "hex.GLMGrid$GLMModels",
    "hex.GLMGrid",
    "hex.Histogram",
    "hex.Histogram$Bins",
    "hex.Histogram$OutlineTask",
    "hex.Histogram$BinningTask",
    "hex.KMeans$KMeansModel",
    "hex.KMeans$Lloyds",
    "hex.KMeans$Sampler",
    "hex.KMeans$Sqr",
    "hex.LinearRegression$CalcRegressionTask",
    "hex.LinearRegression$CalcSquareErrorsTasks",
    "hex.LinearRegression$CalcSumsTask",
    "hex.NewRowVecTask$DataFrame",
    "hex.NewRowVecTask$RowFunc",
    "hex.NewRowVecTask",
    "hex.NOPTask",
    "hex.Plot$Pixels",
    "hex.Plot$Res",
    "hex.rf.Confusion",
    "hex.rf.DRF",
    "hex.rf.MinorityClasses$ClassExtractTask",
    "hex.rf.MinorityClasses$CountMClassRowsTask",
    "hex.rf.MinorityClasses$HistogramTask",
    "hex.rf.MinorityClasses$ReplicationTask",
    "hex.rf.MinorityClasses$UnbalancedClass",
    "hex.rf.RFModel",
    "hex.rf.Tree$1",
    "hex.RowVecTask$Sampling",
    "hex.RowVecTask",
    "water.Atomic",
    "water.AtomicTest$Append$1",
    "water.AutoSerialTest",
    "water.DRemoteTask",
    "water.DTask",
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
    "water.exec.MRColumnProducer",
    "water.exec.MRVectorBinaryOperator",
    "water.exec.MRVectorTernaryOperator",
    "water.exec.MRVectorUnaryOperator",
    "water.exec.MulOperator",
    "water.exec.NeqOperator",
    "water.exec.OrOperator",
    "water.exec.ParametrizedMRVectorUnaryOperator",
    "water.exec.RandBitVect$RandVectBuilder",
    "water.exec.RightAnd",
    "water.exec.RightDiv",
    "water.exec.RightMod",
    "water.exec.RightOr",
    "water.exec.RightSub",
    "water.exec.SliceFilter",
    "water.exec.SubOperator",
    "water.exec.Sum$MRSum",
    "water.exec.TernaryWithScalarOperator",
    "water.exec.TernaryWithTwoScalarsOperator",
    "water.exec.UnaryMinus",
    "water.Freezable",
    "water.H2ONode",
    "water.hdfs.TaskStore2HDFS",
    "water.HeartBeat",
    "water.Iced",
    "water.Job$1",
    "water.Job$2",
    "water.Job$Fail",
    "water.Job$List",
    "water.Job",
    "water.Key",
    "water.Key$Ary",
    "water.KVTest$Atomic2",
    "water.KVTest$ByteHisto",
    "water.KVTest$RemoteBitSet",
    "water.Model",
    "water.MRTask",
    "water.NOPTask",
    "water.parser.DParseTask$AtomicUnion",
    "water.parser.DParseTask",
    "water.parser.Enum",
    "water.parser.ParseDataset$2",
    "water.parser.ParseDataset$3",
    "water.parser.ParseDataset$Progress",
    "water.parser.ParseDataset",
    "water.RReader$RModel",
    "water.store.s3.MultipartUpload$1",
    "water.store.s3.MultipartUpload$Progress",
    "water.store.s3.MultipartUpload",
    "water.TaskGetKey",
    "water.TaskInvalidateKey",
    "water.TaskPutKey",
    "water.TAtomic",
    "water.util.FileIntegrityChecker",
    "water.util.JStackCollectorTask",
    "water.Value",
    "water.ValueArray$Column",
    "water.ValueArray",
    "water.BitsCmpTask"
  };
  static private final HashMap<String,Integer> MAP = new HashMap();
  static {
    int va_id = -1;
    for( int i=0; i<CLAZZES.length; i++ ) {
      MAP.put(CLAZZES[i],i);
      if( CLAZZES[i].equals("water.ValueArray") ) va_id = i;
    }
    VALUE_ARRAY = (short)va_id; // Pre-cached the type id for ValueArray
  }

  static public int onLoad(String className) {
    Integer I = MAP.get(className);
    if(I == null)
      throw new RuntimeException("TypeMap missing " + className);
    return I;
  }

  static private final Freezable[] GOLD = new Freezable[CLAZZES.length];

  static public Iced newInstance(int id) {
    Iced f = (Iced)GOLD[id];
    if( f == null ) {
      try { GOLD[id] = f = (Iced) Class.forName(CLAZZES[id]).newInstance(); }
      catch( Exception e ) { throw new Error(e); }
    }
    return f.newInstance();
  }
  static public Freezable newFreezable(int id) {
    Freezable f = GOLD[id];
    if( f == null ) {
      try { GOLD[id] = f = (Freezable) Class.forName(CLAZZES[id]).newInstance(); }
      catch( Exception e ) { throw new Error(e); }
    }
    return f.newInstance();
  }

  static public String className(int id) { return CLAZZES[id]; }
  static public Class clazz(int id) {
    if( GOLD[id] == null ) newInstance(id);
    return GOLD[id].getClass();
  }

  public static void main(String[] args) {
    Log._dontDie = true; // Ignore class load error, e.g. Request
    File classes = new File(CLASSES);
    ArrayList<String> list = new ArrayList<String>();
    findClasses(classes, list);
    for(String s : list)
      System.out.println("    \"" + s + "\",");
  }
  private static final String CLASSES = "target/classes";
  private static void findClasses(File folder, ArrayList<String> list) {
    for( File file : folder.listFiles() ) {
      if( file.isDirectory() )
        findClasses(file, list);
      else if( file.getPath().endsWith(".class") ) {
        String name = file.getPath().substring(CLASSES.length() + 1);
        name = name.replace('\\', '/').replace('/', '.').replace(".class", "");
        try {
          Class c = Class.forName(name);
          if(Iced.class.isAssignableFrom(c))
            list.add(c.getName());
        } catch(Throwable _) {
          System.out.println("Skipped: " + name);
        }
      }
    }
  }
}
