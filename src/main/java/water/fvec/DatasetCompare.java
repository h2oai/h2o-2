package water.fvec;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutionException;

import hex.glm.GLM2;
import hex.glm.GLMParams;
import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.exec.Env;
import water.exec.Exec2;
import water.parser.ParseDataset;

public class DatasetCompare extends MRTask<DatasetCompare>{
  Frame _fr;
  boolean _eq = true;
  double _diff;
  public static final double compareDatasets(ValueArray ary, Frame fr){
    DatasetCompare tsk = new DatasetCompare();
    tsk._fr = fr;
    tsk.invoke(ary._key);
    System.out.println("diff = " + tsk._diff);
    return tsk._diff;
  }

  @Override public void map(Key key) {
    int cidx = (int)ValueArray.getChunkIndex(key);
    final Key vaKey = ValueArray.getArrayKey(key);
    final Vec [] vecs = _fr.vecs();
    for(int i  = 0; i < vecs.length; ++i)
      assert vecs[i] != null:"missing vector[" + i + "] "+ _fr.names()[i] + ", keys = " + Arrays.toString(_fr._keys) + ", vecs = " + Arrays.toString(_fr.vecs());
    ValueArray va = DKV.get(vaKey).get();
    AutoBuffer bits = va.getChunk(key);
    final int rows = va.rpc(cidx);
    long off = 0;
    for(int i = 0; i < cidx; ++i)
      off += va.rpc(i);
    for(int i = 0; i < rows; ++i){
      for(int j = 0; j < va._cols.length; ++j){
        if(va.isNA(bits, i, j) && vecs[j].isNA(i+off))continue;
        double diff = Math.abs(va.datad(bits, i, j) - vecs[j].at(i+off));
        if(diff > _diff)_diff = diff;
        if(diff >= 1e-3){
          _eq = false;
          System.out.println("incompatible values at row " + (off + i) + " column " + va._cols[j]._name + ", " + va.datad(bits, i, j) + " != " + vecs[j].at(i+off));
        }
      }
    }
  }
  @Override public void reduce(DatasetCompare drt) {
    _eq = _eq && drt._eq;
    _diff = Math.max(_diff,drt._diff);
  }

  private static void do_file(File f,Map<String,Double> diffs,Map<String,Exception> ex){
    if(f.isDirectory()){
      for(File f2:f.listFiles())do_file(f2,diffs,ex);
    } else {
      TestUtil.checkLeakedKeys(); // make sure we have no previous keys in
      System.out.print("parsing file " + f.getPath() + "...");
      Key vaHex = Key.make("va.hex");
      Key vaRaw = null;
      Key frHex = Key.make("fr.hex");
      Key frRaw = null;
      try {
        vaRaw = TestUtil.load_test_file(f,"va.bin");
        try{
          ParseDataset.parse(vaHex, new Key[]{vaRaw});
        }catch(Throwable t){
          System.err.println("Skipping file " + f.getPath() + " as parse1 failed, got exception '" + t.getClass() + "', with msg '" + t.getMessage()+"'");
          if(vaRaw != null)UKV.remove(vaRaw);
          UKV.remove(vaHex);
          return;
        }
        frRaw = NFSFileVec.make(f);
        ParseDataset2.parse(frHex, new Key[]{frRaw});
        ValueArray ary = DKV.get(vaHex).get();
        if(ary.length() > 0){
          Frame fr = DKV.get(frHex).get();
          double d = compareDatasets(ary, fr);
          if(d > 0)diffs.put(f.getPath(),d);
        }
      }catch(Exception e){
        ex.put(f.getPath(), e);
      }finally{
        if(vaRaw != null)UKV.remove(vaRaw);
        for(Key k:H2O.localKeySet())UKV.remove(k);
        UKV.remove(vaHex);
        UKV.remove(frHex);
        if(frRaw != null)UKV.remove(frRaw);
      }
    }
  }

  // to be parsed when cloud is up
  private static String [] datasets = new String[] {
    "/Users/tomasnykodym/h2o/smalldata/logreg/prostate.csv","prostate",
    "/Users/tomasnykodym/mydata/arcene/arcene_train.data","train_raw",
    "/Users/tomasnykodym/mydata/arcene/arcene_train_labels.labels","labels",
    "/Users/tomasnykodym/mydata/coll/train.csv","dtrain",
    "/Users/tomasnykodym/mydata/coll/test.csv","dtest"
  };

  private static String [] cmds = new String[] {
    "train2 <- cbind(train_raw,labels);","train",
  };

  public static void main(String [] args) throws InterruptedException, ExecutionException{
    System.out.println("Running Parser Cmp Test with args=" + Arrays.toString(args));
    System.out.println(Arrays.equals((int[])null,(int[])null));
    final int nnodes = 1;
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes);
    for(int i = 0; i < datasets.length; i += 2){
      File f = new File(datasets[i]);
      if(!f.exists()) throw new RuntimeException("did not find file " + f.getAbsolutePath());
      Key frRaw = NFSFileVec.make(f);
      Key frHex = Key.make(datasets[i+1]);
      ParseDataset2.parse(frHex, new Key[]{frRaw});
    }
    Futures fs = new Futures();
    for(int i = 0; i < cmds.length; i += 2){
      System.out.println("Executing: '" + cmds[i] + "'");
      Env env = Exec2.exec(cmds[i]);
      Frame fr = env.popAry();
      DKV.put(Key.make(cmds[i+1]),fr,fs);
    }
    fs.blockForPending();
//    new GLMTest2().testCars();
//    File f = new File("/Users/tomasnykodym/h2o/smalldata/airlines/allyears2k_headers.csv");
//    assert f.exists():"did not find file " + f.getPath();
//    Key frRaw = NFSFileVec.make(f);
//    Key frHex = Key.make("allyears2k_headers.hex");
//    ParseDataset2.parse(frHex, new Key[]{frRaw});
//    GLM2 glm = new GLM2();
//    glm.ignored_cols = new int[]{4,6,10,11,13,14,15,19,20,21,22,23,24,25,26,27,28,29};
//    glm.source = DKV.get(frHex).get();
//    glm._glm = new GLMParams(GLMParams.Family.binomial, 0, GLMParams.Link.logit, 0);
//    glm.response = glm.source.vec("IsDepDelayed");
//    glm.job_key = Key.make();
//    glm.destination_key = Key.make();
//    glm.lambda = new double[]{1e-5};
//    long t1 = System.currentTimeMillis();
//    glm.init();
//    glm.fork().get();
//    System.out.println("glm done in " + (System.currentTimeMillis()-t1) + "ms");
    String root = (args.length > 0)?args[0]:"smalldata";
    System.out.println("ROOT = " + root);
    System.out.println("Running...");
//    new NewVectorTest().testCompression();
////    Map<String,Double> diffs = new TreeMap<String, Double>();
////    Map<String,Exception> exs = new TreeMap<String, Exception>();
////    do_file(TestUtil.find_test_file(root),diffs,exs);
////    System.out.println("DONE!!!");
//    File f = new File("/Users/tomasnykodym/mydata/c12/anon_file_bigger.csv");
//    assert f.exists():"did not find file " + f.getPath();
//    Key frRaw = NFSFileVec.make(f);
//    Key frHex = Key.make("anon_file_bigger.hex");
//    ParseDataset2.parse(frHex, new Key[]{frRaw});
//    File f2 = new File("/Users/tomasnykodym/h2o/smalldata/airlines/AirlinesTrain.csv.zip");
//    assert f2.exists():"did not find file " + f.getPath();
//    Key frRaw2 = NFSFileVec.make(f2);
//    Key frHex2 = Key.make("allyears2k_headers.hex");
//    ParseDataset2.parse(frHex2, new Key[]{frRaw2});
    System.out.println("DONE");
//    for(Map.Entry<String, Exception> e:exs.entrySet()){
//      System.err.println("Exception occured while processing " + e.getKey());
//      e.getValue().printStackTrace();
//    }
//    for(Map.Entry<String, Double> e:diffs.entrySet())
//      if(e.getValue() > 1e-5)System.err.println(e.getKey() + ", DIFF = " + e.getValue());
//    System.err.flush();
//    for(Map.Entry<String, Double> e:diffs.entrySet())
//      System.out.println(e.getKey() + ", diff = " + e.getValue());
  }
}
