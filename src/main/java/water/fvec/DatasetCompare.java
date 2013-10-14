package water.fvec;

import java.io.File;
import java.util.Arrays;

import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.parser.ParseDataset;
import static org.junit.Assert.assertEquals;

public class DatasetCompare extends MRTask<DatasetCompare>{
  Frame _fr;
  boolean _eq = true;
  double _diff;
  public static final boolean compareDatasets(ValueArray ary, Frame fr){
    DatasetCompare tsk = new DatasetCompare();
    tsk._fr = fr;
    tsk.invoke(ary._key);
    System.out.println("diff = " + tsk._diff);
    return tsk._eq;
  }

  @Override public void map(Key key) {
    int cidx = (int)ValueArray.getChunkIndex(key);
    final Key vaKey = ValueArray.getArrayKey(key);
    final Vec [] vecs = _fr.vecs();
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
        if(diff >= 1e-1){
          _eq = false;
//          System.out.println("incompatible values at row " + (off + i) + " column " + va._cols[j]._name + ", " + va.datad(bits, i, j) + " != " + vecs[j].at(i+off));
        }
      }
    }
  }
  @Override public void reduce(DatasetCompare drt) {
    _eq = _eq && drt._eq;
    _diff = Math.max(_diff,drt._diff);
  }

  private static void do_file(File f){
    if(f.isDirectory()){
      for(File f2:f.listFiles())do_file(f2);
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
        Frame fr = DKV.get(frHex).get();
        assertEquals(true,compareDatasets(ary, fr));
      }finally{
        if(vaRaw != null)UKV.remove(vaRaw);
        UKV.remove(vaHex);
        UKV.remove(frHex);
        if(frRaw != null)UKV.remove(frRaw);
      }
    }
  }
  public static void main(String [] args){
    System.out.println("Running Parser Cmp Test with args=" + Arrays.toString(args));
    final int nnodes = 3;
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes);
    String root = (args.length > 0)?args[0]:"smalldata";
    System.out.println("ROOT = " + root);
    System.out.println("Running...");
//    new water.fvec.FVecTest().testLargeCats();
    // do smalldata
    do_file(TestUtil.find_test_file(root));
//    do_file(TestUtil.find_test_file("smalldata/categoricals/40k_categoricals.csv.gz"));
    System.out.println("DONE!!!");
  }
}
