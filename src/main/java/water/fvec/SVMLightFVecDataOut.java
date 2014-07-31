package water.fvec;

import java.util.Arrays;
import water.fvec.ParseDataset2.FVecDataOut;
import water.fvec.Vec.VectorGroup;
import water.parser.Enum;

public class SVMLightFVecDataOut extends FVecDataOut {
  protected final VectorGroup _vg;
  public SVMLightFVecDataOut(VectorGroup vg, int cidx, int ncols, int vecIdStart, Enum [] enums){
    super(vg,cidx,0,vg.reserveKeys(10000000),enums);
    _nvs = new NewChunk[0];
    _vg = vg;
    _col = 0;
  }

  private void addColumns(int ncols){
    if(ncols > _nCols){
      _nvs   = Arrays.copyOf(_nvs   , ncols);
      _vecs  = Arrays.copyOf(_vecs  , ncols);
      _ctypes= Arrays.copyOf(_ctypes, ncols);
      for(int i = _nCols; i < ncols; ++i) {
        _vecs[i] = new AppendableVec(_vg.vecKey(i+1));
        _nvs[i] = new NewChunk(_vecs[i], _cidx);
        for(int j = 0; j < _nLines; ++j)
          _nvs[i].addNum(0, 0);
      }
      _nCols = ncols;
    }
  }
  @Override public void addNumCol(int colIdx, long number, int exp) {
    assert colIdx >= _col;
    addColumns(colIdx+1);
    for(int i = _col; i < colIdx; ++i)
      super.addNumCol(i, 0, 0);
    super.addNumCol(colIdx, number, exp);
    _col = colIdx+1;
  }
  @Override
  public void newLine() {
    if(_col < _nCols)addNumCol(_nCols-1, 0,0);
    super.newLine();
    _col = 0;
  }

}
