package hex.la;

import water.H2O;
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
    if(xcols != yrows)
      throw new IllegalArgumentException("Matrices are not compatible for multiplication: ["+xrows+"x"+xcols+"] * ["+yrows+"x"+ycols+"]. Requires [n x m] * [m x p]");

    Vec[] x_vecs = _x.vecs();
    Vec[] y_vecs =  y.vecs();
    for(int k = 0; k < xcols; k++) {
      if(x_vecs[k].isEnum())
        throw new IllegalArgumentException("Multiplication not meaningful for factor column "+k);
    }
    for(int j = 0; j < ycols; j++) {
      if(y_vecs[j].isEnum())
        throw new IllegalArgumentException("Multiplication not meaningful for factor column "+j);
    }

    Vec[] output = new Vec[ycols];
    for(int j = 0; j < ycols; j++)
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
    return new Frame(y._names,output);
  }

  // Outer product
  public Frame outerProd() {
    int xrows = (int)_x.numRows();
    int xcols =      _x.numCols();
    Vec[] x_vecs = _x.vecs();

    for(int j = 0; j < xcols; j++) {
      if(x_vecs[j].isEnum())
        throw new IllegalArgumentException("Multiplication not meaningful for factor column "+j);
    }

    Vec[] output = new Vec[xrows];
    String[] names = new String[xrows];
    for(int i = 0; i < xrows; i++) {
      output[i] = Vec.makeSeq(xrows);
      names[i] = "C" + String.valueOf(i+1);
    }

    for(int i = 0; i < xrows; i++) {
      for(int j = 0; j < xrows; j++) {
        double d = 0;
        for(int k = 0; k < xcols; k++)
          d += x_vecs[k].at(i)*x_vecs[k].at(k);
        output[j].set(i, d);
      }
    }
    return new Frame(names, output);
  }

  // Transpose
  public Frame trans() {
    int xrows = (int)_x.numRows();
    int xcols =      _x.numCols();
    Vec[] x_vecs = _x.vecs();

    // Currently cannot transpose factors due to domain mismatch
    for(int j = 0; j < xcols; j++) {
      if(x_vecs[j].isEnum())
        throw H2O.unimpl();
    }

    Vec[] output = new Vec[xrows];
    String[] names = new String[xrows];
    for(int i = 0; i < xrows; i++) {
      output[i] = Vec.makeSeq(xcols);
      names[i] = "C" + String.valueOf(i+1);
    }

    for(int i = 0; i < xrows; i++) {
      for(int j = 0; j < xcols; j++) {
        double d = x_vecs[j].at(i);
        output[i].set(j, d);
      }
    }
    return new Frame(names, output);
  }
}
