package water.fvec;

import java.util.Arrays;

import water.Futures;
import water.fvec.ParseDataset2.FVecDataOut;
import water.fvec.Vec.VectorGroup;
import water.parser.Enum;

public class SVMLightFVecDataOut extends FVecDataOut {
  protected final VectorGroup _vg;
  final int _vecIdStart;




  public SVMLightFVecDataOut(VectorGroup vg, int cidx, AppendableVec [] avs, int vecIdStart, int chunkOff,  Enum [] enums){
    super(vg,chunkOff,cidx,enums, avs);
    _vg = vg;
    _vecIdStart = vecIdStart;
    _nvs = new NewChunk[avs.length];
    for(int i = 0; i < _nvs.length; ++i)
      _nvs[i] = new NewChunk(_vecs[i], _cidx, true);
    _ctypes= new byte[avs.length];
    _col = 0;
  }

  private void addColumns(int ncols){
    if(ncols > _nvs.length){
      int _nCols = _vecs.length;
      _nvs   = Arrays.copyOf(_nvs   , ncols);
      _vecs  = Arrays.copyOf(_vecs  , ncols);
      _ctypes= Arrays.copyOf(_ctypes, ncols);

      for(int i = _nCols; i < ncols; ++i) {
        _vecs[i] = new AppendableVec(_vg.vecKey(i+_vecIdStart),_vecs[0]._espc, _chunkOff);
        _nvs[i] = new NewChunk(_vecs[i], _cidx, true);
      }
    }
  }
  @Override public void addNumCol(int colIdx, long number, int exp) {
    assert colIdx >= _col;
    if(colIdx >= _vecs.length) addColumns(colIdx+1);
    _nvs[colIdx].addZeros((int)_nLines - _nvs[colIdx]._len);
    _nvs[colIdx].addNum(number, exp);
    if(_ctypes[colIdx] == UCOL ) _ctypes[colIdx] = NCOL;
    _col = colIdx+1;
  }

  @Override
  public void newLine() {
    ++_nLines;
    _col = 0;
  }

  @Override public final void addInvalidCol(int colIdx) {
    assert colIdx >= _col;
    if(colIdx >= _vecs.length) addColumns(colIdx+1);
    _nvs[colIdx].addZeros((int)_nLines - _nvs[colIdx]._len);
    _nvs[colIdx].addNA();
    _col = colIdx+1;
  }
  @Override public FVecDataOut close(Futures fs) {
    for(NewChunk nc:_nvs) {
      nc.addZeros((int) _nLines - nc._len);
      assert nc._len == _nLines:"incompatible number of lines after parsing chunk, " + _nLines + " != " + nc._len;
    }
    return super.close(fs);
  }


}
