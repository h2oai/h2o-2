package water.exec;

import water.*;

// =============================================================================
// MRVectorBinaryOperator
// =============================================================================
/**
 *
 * @author peta
 */
public abstract class MRVectorBinaryOperator extends MRColumnProducer {

  private final Key _leftKey;
  private final Key _rightKey;
  private final Key _resultKey;
  private final int _leftCol;
  private final int _rightCol;

  /**
   * Creates the binary operator task for the given keys.
   *
   * All keys must be created beforehand and they represent the left and right
   * operands and the result. They are all expected to point to ValueArrays.
   *
   * @param left
   * @param right
   * @param result
   */
  public MRVectorBinaryOperator(Key left, Key right, Key result, int leftCol, int rightCol) {
    _leftKey = left;
    _rightKey = right;
    _resultKey = result;
    _leftCol = leftCol;
    _rightCol = rightCol;
  }

  /**
   * This method actually does the operation on the data itself.
   *
   * @param left Left operand
   * @param right Right operand
   * @return left operator right
   */
  public abstract double operator(double left, double right);


  /* We are creating new one, so I can tell the row number quite easily from the
   * chunk index.
   */
  @Override public void map(Key key) {
    ValueArray result = ValueArray.value(_resultKey);
    long cidx = ValueArray.getChunkIndex(key);
    long rowOffset = result.startRow(cidx);
    VAIterator left = new VAIterator(_leftKey,_leftCol, rowOffset);
    VAIterator right = new VAIterator(_rightKey,_rightCol, rowOffset);
    int chunkRows = result.rpc(cidx);
    AutoBuffer bits = new AutoBuffer(chunkRows * 8);
    for( int i = 0; i < chunkRows; i++ ) {
      left.next();
      right.next();
      double x = operator(left.datad(), right.datad());
      bits.put8d(x);
      updateColumnWith(x);
    }
    Value val = new Value(key, bits.bufClose());
    DKV.put(key, val, getFutures());
  }

}

// =============================================================================
// AddOperator
// =============================================================================
class AddOperator extends water.exec.MRVectorBinaryOperator {

  public AddOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left + right; }
}

// =============================================================================
// SubOperator
// =============================================================================
class SubOperator extends water.exec.MRVectorBinaryOperator {

  public SubOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left - right; }
}

// =============================================================================
// MulOperator
// =============================================================================
class MulOperator extends water.exec.MRVectorBinaryOperator {

  public MulOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left * right; }
}

// =============================================================================
// DivOperator
// =============================================================================
class DivOperator extends water.exec.MRVectorBinaryOperator {

  public DivOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left / right; }
}

// =============================================================================
// ModOperator
// =============================================================================
class ModOperator extends water.exec.MRVectorBinaryOperator {

  public ModOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left % right; }
}

// =============================================================================
// LessOperator
// =============================================================================
class LessOperator extends water.exec.MRVectorBinaryOperator {

  public LessOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left < right ? 1 : 0; }
}

// =============================================================================
// LessOrEqOperator
// =============================================================================
class LessOrEqOperator extends water.exec.MRVectorBinaryOperator {

  public LessOrEqOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left <= right ? 1 : 0; }
}

// =============================================================================
// GreaterOperator
// =============================================================================
class GreaterOperator extends water.exec.MRVectorBinaryOperator {

  public GreaterOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left > right ? 1 : 0; }
}

// =============================================================================
// GreaterOrEqOperator
// =============================================================================
class GreaterOrEqOperator extends water.exec.MRVectorBinaryOperator {

  public GreaterOrEqOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left >= right ? 1 : 0; }
}

// =============================================================================
// EqOperator
// =============================================================================
class EqOperator extends water.exec.MRVectorBinaryOperator {

  public EqOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left == right ? 1 : 0; }
}

// =============================================================================
// NeqOperator
// =============================================================================
class NeqOperator extends water.exec.MRVectorBinaryOperator {

  public NeqOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left != right ? 1 : 0; }
}

// =============================================================================
// AndOperator
// =============================================================================
class AndOperator extends water.exec.MRVectorBinaryOperator {

  public AndOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return ((left != 0) &&  (right != 0)) ? 1 : 0; }
}

// =============================================================================
// OrOperator
// =============================================================================
class OrOperator extends water.exec.MRVectorBinaryOperator {

  public OrOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return ((left != 0) || (right != 0)) ? 1 : 0; }
}

