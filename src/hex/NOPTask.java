package hex;
import water.*;

public class NOPTask extends MRTask {

  public int _res;
  public int _missingKeysCount;
  public long _minMissingKey = Integer.MAX_VALUE;
  public long _maxMissingKey = Integer.MIN_VALUE;

  @Override
  public void map(Key key) {
    Value v = DKV.get(key);
    if(v != null){
      byte [] mem = DKV.get(key).get();
      for(byte b:mem){
        _res ^= b;
      }
    } else {
      ++_missingKeysCount;
      _minMissingKey = ValueArray.getChunkIndex(key);
      _maxMissingKey = _minMissingKey;
    }
  }

  @Override
  public void reduce(DRemoteTask drt) {
    NOPTask other = (NOPTask)drt;
    _res ^= other._res;
    _missingKeysCount += other._missingKeysCount;
    _minMissingKey = Math.min(_minMissingKey, other._minMissingKey);
    _maxMissingKey = Math.max(_maxMissingKey, other._maxMissingKey);
  }

  public String toString(){
    if(_missingKeysCount > 0)
      return _missingKeysCount + " keys missing, Min = " + _minMissingKey + ", Max = " + _maxMissingKey + ", byte xor = " + _res;
    return "No keys were missing. Byte xor = " + _res;
  }
}
