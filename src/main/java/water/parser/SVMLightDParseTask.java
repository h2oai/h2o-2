package water.parser;

import java.util.Arrays;

public class SVMLightDParseTask extends DParseTask {
  public SVMLightDParseTask() {}
  @Override
  public void newLine() {
    if (_colIdx < _ncolumns)
      addNumCol(_ncolumns-1,0,0);
    super.newLine();
    _colIdx = 0;
  }  private int _colIdx = 0;

  @Override
  public void addColumns(int ncols){
    _colTypes = Arrays.copyOf(_colTypes, ncols);
    _min = Arrays.copyOf(_min, ncols); // additional columns has min/max/mean 0
    _max = Arrays.copyOf(_max, ncols);
    if(_myrows == 0) { // in this case we can not be sure if 0 ever occurs in this particular column
      _min[ncols-1] = Double.POSITIVE_INFINITY;
      _max[ncols-1] = Double.NEGATIVE_INFINITY;
    }
    _scale = Arrays.copyOf(_scale, ncols);
    _mean = Arrays.copyOf(_mean, ncols);
    _invalidValues = Arrays.copyOf(_invalidValues, ncols);
    _ncolumns = ncols;
    createEnums();
  }

  @Override
  public void addNumCol(int colIdx, long number, int exp){
    if(_phase == Pass.ONE && colIdx >= _ncolumns)
      addColumns(colIdx+1);
    assert colIdx >= _colIdx;
    for(int i = _colIdx; i < colIdx; ++i)
      super.addNumCol(i, 0,0);
    super.addNumCol(colIdx, number, exp);
    _colIdx = colIdx+1;
  }
  public void addInvalidCol(int colIdx){
  }
  @Override
  public void addStrCol( int colIdx, ValueString str ) {
    throw new UnsupportedOperationException();
  }
  @Override public void reduce(DParseTask dpt){
    if(dpt._ncolumns > _ncolumns && _map){
      int n = _ncolumns;
      addColumns(dpt._ncolumns); // not effective, quick solution for now...
    }
    super.reduce(dpt);
  }

  @Override public void createValueArrayHeader(){
    String [] colNames = new String[_ncolumns];
    colNames[0] = "Target";
    for(int i = 1; i < _ncolumns; ++i)
      colNames[i] = "V" + i;
    _colNames = colNames;
    super.createValueArrayHeader();
  }
  @Override protected void createEnums() {} // SVMlight has no strings, we don't need any enums
}
