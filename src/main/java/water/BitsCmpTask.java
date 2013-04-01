package water;

import java.util.Arrays;

public class BitsCmpTask extends MRTask {
  final Key _origDataset;
  boolean _res = true;

  public BitsCmpTask(ValueArray origAry){
    _origDataset = origAry._key;
  }
  @Override
  public void map(Key key) {
    ValueArray origAry = DKV.get(_origDataset).get();
    long idx = ValueArray.getChunkIndex(key);
    _res = Arrays.equals(DKV.get(key).getBytes(), DKV.get(origAry.getChunkKey(idx)).getBytes());
  }

  @Override
  public void reduce(DRemoteTask drt) {
    _res &= ((BitsCmpTask)drt)._res;
  }
}
