package hex;

import water.*;
import water.ValueArray.Column;
import water.fvec.Frame;
import water.util.Utils;

public class ScoreTask extends MRTask {

  final OldModel _M;
  final Key _outKey;
  final int _nchunks;
  private double _min = Double.POSITIVE_INFINITY;
  private double _max = Double.NEGATIVE_INFINITY;
  long _n;
  private double _sum;
  private double _sumsq;

  private int [] _rpc;

  private ScoreTask(OldModel M, Key outKey, int nchunks){
    _M = M;
    _outKey = outKey;
    _nchunks = nchunks;
  }

  public static Key score(OldModel M, ValueArray data, Key outputKey){
    ScoreTask t = new ScoreTask(M.adapt(data),outputKey,(int)data.chunks());
    t.invoke(data._key);
    Column c = new Column();
    c._max = t._max;
    c._min = t._min;
    c._mean = t._sum/t._n;
    c._n = t._n;
    double norm = 1.0/t._n;
    double s = t._sum*norm;
    c._sigma =  Math.sqrt(t._sumsq*norm - s*s);
    c._name = M.responseName();
    c._size = -8;
    c._scale = 1;
    c._base = 0;
    c._off = 0;
    c._domain = null;
    ValueArray res = new ValueArray(outputKey,t._rpc,8, new Column[]{c});
    DKV.put(outputKey, res);
    return res._key;
  }

  @Override public void init(){
    _rpc = new int[_nchunks];
    super.init();
  }
  @Override public void map(Key key) {
    final OldModel m = (OldModel)_M.clone();
    ValueArray ary = DKV.get(ValueArray.getArrayKey(key)).get();
    AutoBuffer bits = new AutoBuffer(DKV.get(key).memOrLoad());
    int nrows = bits.remaining()/ary._rowsize;
    AutoBuffer res = new AutoBuffer(nrows<<3);
    for(int i = 0; i< nrows; ++i){
      double p = m.score(ary, bits, i);
      if(p < _min)_min = p;
      if(p > _max)_max = p;

      if(!Double.isNaN(p)){
        _sum += p;
        _sumsq += p*p;
        ++_n;
      }
      res.put8d(p);
    }
    int idx = (int)ValueArray.getChunkIndex(key);
    Key outputKey = ValueArray.getChunkKey(idx,_outKey);
    _rpc[idx] += nrows;
    DKV.put(outputKey, new Value(outputKey,res.buf()));
  }

  @Override public void reduce(DRemoteTask drt) {
    ScoreTask t = (ScoreTask)drt;
    _min = Math.min(_min,t._min);
    _max = Math.max(_max,t._max);
    _sum += t._sum;
    _sumsq += t._sumsq;
    _n += t._n;
    if(_rpc == null)
      _rpc = t._rpc;
    else if(_rpc != t._rpc)
      Utils.add(_rpc,t._rpc);
  }
}
