package hex.la;

import water.fvec.*;

public final class Matrix {
  final Frame _x;

  public Matrix(Frame x) { _x = x; }

  // Matrix multiplication
  public Frame mult(Frame y) {
    int xrows = (int)_x.numRows();
    int xcols =      _x.numCols();
    int yrows = (int) y.numRows();
    int ycols =       y.numCols();
    if( xcols != yrows )
      throw new IllegalArgumentException("Matrices do not match: ["+xrows+"x"+xcols+"] * ["+yrows+"x"+ycols+"]");
    Vec[] x_vecs = _x.vecs();
    Vec[] y_vecs =  y.vecs();
    Vec[] output = new Vec[ycols];
    for( int j=0; j<ycols; j++ )
      output[j] = Vec.makeSeq(xrows);

    for(int i = 0; i < xrows; i++) {
      for(int j = 0; j < ycols; j++) {
        Vec yvec = y_vecs[j];
        double d = 0;
        for(int k = 0; k < xcols; k++)
          d += x_vecs[k].at(i) * yvec.at(k);
        output[j].set(i, d);
      }
    }
    return new Frame(_x._names,output);
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
    return new Frame(output);
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
    return new Frame(output);
  }
}
