package hex.la;

import water.fvec.*;

public final class Matrix {
  final Frame _x;

  public Matrix(Frame x) {
    _x = x;
  }

  // Matrix multiplication
  public Frame mult(Frame _y) {
    assert _x.numCols() == (int)_y.numRows();
    Vec[] x_vecs = _x.vecs();
    Vec[] y_vecs = _y.vecs();
    Vec[] output = new Vec[_y.numCols()];

    for(int i = 0; i < _x.numRows(); i++) {
      for(int j = 0; j < _y.numCols(); j++) {
        double d = 0;
        for(int k = 0; k < _x.numCols(); k++) {
            d += x_vecs[k].at(i)*y_vecs[j].at(k);
        }
        output[j].set(i, d);
      }
    }
    return(new Frame(output));
  }

  // Outer product
  public Frame outerProd() {
    Vec[] x_vecs = _x.vecs();
    Vec[] output = new Vec[(int)_x.numRows()];
    for(int i = 0; i < _x.numRows(); i++) {
      for(int j = 0; j < _x.numRows(); j++) {
        double d = 0;
        for(int k = 0; k < _x.numCols(); k++)
          d += x_vecs[k].at(i)*x_vecs[k].at(k);
        output[j].set(i, d);
      }
    }
    return(new Frame(output));
  }

  // Transpose
  public Frame trans() {
    Vec[] x_vecs = _x.vecs();
    Vec[] output = new Vec[(int)_x.numRows()];

    for(int i = 0; i < _x.numRows(); i++) {
      for(int j = 0; j < _x.numCols(); j++) {
        double d = x_vecs[j].at(i);
        output[i].set(j, d);
      }
    }
    return(new Frame(output));
  }
}
