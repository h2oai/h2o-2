package water.fvec;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutionException;

import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;
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
  public static void main(String [] args) throws InterruptedException, ExecutionException{
    System.out.println("Running Parser Cmp Test with args=" + Arrays.toString(args));
    final int nnodes = 1;
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes);
    String root = (args.length > 0)?args[0]:"smalldata";
    System.out.println("ROOT = " + root);
    System.out.println("Running...");
    new NewVectorTest().testCompression();
    Map<String,Double> diffs = new TreeMap<String, Double>();
    Map<String,Exception> exs = new TreeMap<String, Exception>();
    do_file(TestUtil.find_test_file(root),diffs,exs);
    System.out.println("DONE!!!");
    for(Map.Entry<String, Exception> e:exs.entrySet()){
      System.err.println("Exception occured while processing " + e.getKey());
      e.getValue().printStackTrace();
    }
    for(Map.Entry<String, Double> e:diffs.entrySet())
      if(e.getValue() > 1e-5)System.err.println(e.getKey() + ", DIFF = " + e.getValue());
    System.err.flush();
    for(Map.Entry<String, Double> e:diffs.entrySet())
      System.out.println(e.getKey() + ", diff = " + e.getValue());
  }
}
