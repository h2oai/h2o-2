package water.fvec;

import java.util.Arrays;

import water.Futures;
import water.fvec.ParseDataset2.FVecDataOut;
import water.fvec.Vec.VectorGroup;
import water.parser.Enum;


public class SVMLightFVecDataOut extends FVecDataOut {
  final VectorGroup _vg;
  private int _colIdx;
  private final int _cidx;
  private AppendableVec [] _vecs = new AppendableVec[0];

  SVMLightFVecDataOut(Chunk in, Enum [] enums){
    super(new NewChunk[0],enums);
    _nvs = new NewChunk[0];
    _vg = in._vec.group();
    _vg.reserveKeys(1000000); // reserve million keys
    _cidx = in.cidx();
  }

  private void addColumns(int ncols){
    if(ncols > _nCols){
      _nvs = Arrays.copyOf(_nvs, ncols);
      _vecs = Arrays.copyOf(_vecs, ncols);
      for(int i = _nCols; i < ncols; ++i){
        _vecs[i] = new AppendableVec(_vg.vecKey(i+1));
        _nvs[i] = new NewChunk(_vecs[i], _cidx);
        for(int j = 0; j < _nLines; ++j)
          _nvs[i].addNum(0, 0);
      }
      _nCols = ncols;
    }
  }
  @Override public void addNumCol(int colIdx, long number, int exp) {
    addColumns(colIdx+1);
    assert colIdx >= _colIdx;
    for(int i = _colIdx; i < colIdx; ++i)
      super.addNumCol(i, 0, 0);
    super.addNumCol(colIdx, number, exp);
    _colIdx = colIdx+1;
  }
  @Override
  public void newLine() {
    if(_colIdx < _nCols)addNumCol(_nCols-1, 0,0);
    super.newLine();
    _colIdx = 0;
  }
  public void reduce(SVMLightFVecDataOut dout){
    if(dout._vecs.length > _vecs.length){
      AppendableVec [] v = _vecs;
      _vecs = dout._vecs;
      dout._vecs = v;
    }
    for(int i = 0; i < dout._vecs.length; ++i)
      _vecs[i].reduce(dout._vecs[i]);
  }
  public void close(Futures fs){
    for(NewChunk nv:_nvs)
      nv.close(_cidx, fs);
  }
  public Vec [] closeVecs(Futures fs){
    Vec [] res = new Vec[_vecs.length];
    for(int i = 0; i < _vecs.length; ++i)
      res[i] = _vecs[i].close(fs);
    return res;
  }
}
