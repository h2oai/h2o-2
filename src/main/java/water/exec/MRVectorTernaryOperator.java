package water.exec;

import water.*;

// =============================================================================
// MRVectorTernaryOperator
// =============================================================================
/**
 *
 * @author peta
 */
public abstract class MRVectorTernaryOperator extends MRColumnProducer {

  private final Key _opnd1Key;
  private final int _opnd1Col;
  private final Key _opnd2Key;
  private final int _opnd2Col;
  private final Key _opnd3Key;
  private final int _opnd3Col;
  private final Key _resultKey;

  public MRVectorTernaryOperator(Key opnd1, Key opnd2, Key opnd3, Key result, int col1, int col2, int col3) {
    _opnd1Key = opnd1;
    _opnd1Col = col1;
    _opnd2Key = opnd2;
    _opnd2Col = col2;
    _opnd3Key = opnd3;
    _opnd3Col = col3;
    _resultKey = result;
  }

  /**
   * This method actually does the operation on the data itself.
   *
   * @param left Left operand
   * @param right Right operand
   * @return left operator right
   */
  public abstract double operator(double opnd1, double opnd2, double opnd3);


  /* We are creating new one, so I can tell the row number quite easily from the
   * chunk index.
   */
  @Override public void map(Key key) {
    ValueArray result = ValueArray.value(_resultKey);
    long cidx = ValueArray.getChunkIndex(key);
    long rowOffset = result.startRow(cidx);
    VAIterator op1 = new VAIterator(_opnd1Key, _opnd1Col, rowOffset);
    VAIterator op2 = new VAIterator(_opnd2Key, _opnd2Col, rowOffset);
    VAIterator op3 = new VAIterator(_opnd3Key, _opnd3Col, rowOffset);
    int chunkRows = result.rpc(cidx);
    AutoBuffer bits = new AutoBuffer(chunkRows * 8);
    for (int i = 0; i < chunkRows; i++) {
      op1.next();
      op2.next();
      op3.next();
      double x = operator(op1.datad(), op2.datad(), op3.datad());
      bits.put8d(x);
      updateColumnWith(x);
    }
    Value val = new Value(key, bits.bufClose());
    DKV.put(key, val, getFutures());
  }

}

// =============================================================================
// Ternary operator with scalar argument
// =============================================================================

abstract class TernaryWithScalarOperator extends MRVectorBinaryOperator {
  public final double _opndS;
  public TernaryWithScalarOperator(Key opnd1, Key opnd2, double opndS, Key result, int col1, int col2) {
    super(opnd1, opnd2, result, col1, col2);
    _opndS = opndS;
  }
}

// =============================================================================
// Ternary operator with two scalar arguments
// =============================================================================

abstract class TernaryWithTwoScalarsOperator extends MRVectorUnaryOperator {

  public final double _opnd2;
  public final double _opnd3;

  public TernaryWithTwoScalarsOperator(Key opnd1, double opnd2, double opnd3, Key result, int col1) {
    super(opnd1,result,col1);
    _opnd2 = opnd2;
    _opnd3 = opnd3;
  }
}

// =============================================================================
// IifOperator
// =============================================================================

class IifOperator extends MRVectorTernaryOperator {

  public IifOperator(Key opnd1, Key opnd2, Key opnd3, Key result, int col1, int col2, int col3) {
    super(opnd1, opnd2, opnd3, result, col1, col2, col3);
  }

  @Override public double operator(double opnd1, double opnd2, double opnd3) {
    return opnd1 != 0 ? opnd2 : opnd3;
  }
}

// =============================================================================
// IifOperatorScalar2
// =============================================================================

class IifOperatorScalar2 extends TernaryWithScalarOperator {

  public IifOperatorScalar2(Key opnd1, double opnd2, Key opnd3, Key result, int col1, int col3) {
    super(opnd1, opnd3, opnd2, result, col1, col3);
  }

  @Override public double operator(double opnd1, double opnd3) {
    return opnd1 != 0 ? _opndS : opnd3;
  }
}

// =============================================================================
// IifOperatorScalar3
// =============================================================================

class IifOperatorScalar3 extends TernaryWithScalarOperator {

  public IifOperatorScalar3(Key opnd1, Key opnd2, double opnd3, Key result, int col1, int col2) {
    super(opnd1, opnd2, opnd3, result, col1, col2);
  }

  @Override public double operator(double opnd1, double opnd2) {
    return opnd1 != 0 ? opnd2 : _opndS;
  }
}

// =============================================================================
// IifOperatorScalar23
// =============================================================================

class IifOperatorScalar23 extends TernaryWithTwoScalarsOperator {

  public IifOperatorScalar23(Key opnd1, double opnd2, double opnd3, Key result, int col1) {
    super(opnd1, opnd2, opnd3, result, col1);
  }

  @Override public double operator(double opnd1) {
    return opnd1 != 0 ? _opnd2 : _opnd3;
  }

}

