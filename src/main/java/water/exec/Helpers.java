package water.exec;

import java.io.IOException;

import com.google.common.base.Throwables;

import water.*;
import water.ValueArray.Column;
import water.exec.Expr.Result;
import water.parser.ParseDataset;

/**
 *
 * @author peta
 */
public class Helpers {


  public static int checkedColumnIndex(ValueArray ary, Result from) {
    int result = -1;
    if (from._type == Result.Type.rtStringLiteral) {
      for (int i = 0; i < ary.numCols(); ++i)
        if (from._str.equals(ary._cols[i]._name))
          return i;
    } else {
      result = (int) from._const;
    }
    if (result >= ary.numCols())
      return -1;
    return result;
  }


  // scalar collector task -----------------------------------------------------

  public static abstract class ScallarCollector extends MRTask {

    public final Key _key;
    public int _col;
    protected double _result;

    protected abstract void collect(double x);

    protected abstract void reduce(double x);

    public double result() { return _result; }

    @Override public void map(Key key) {
      _result = 0;
      ValueArray va = ValueArray.value(_key);
      Column c = va._cols[_col];
      AutoBuffer bits = va.getChunk(key);
      int rowSize = va._rowsize;
      for( int i = 0; i < bits.remaining() / rowSize; ++i ) {
        double x = va.datad(bits, i, c);
        if (!Double.isNaN(x))
          collect(x);
      }
    }

    @Override public void reduce(DRemoteTask drt) {
      Helpers.ScallarCollector other = (Helpers.ScallarCollector) drt;
      if (!Double.isNaN(other._result))
        reduce(other._result);
    }

    public ScallarCollector(Key key, int col, double initVal) { // constructor
      _key = key;
      _col = col >=0 ? col : 0;
      _result = initVal;
    }
  }




  // sigma ---------------------------------------------------------------------

  /**
   * Calculates the second pass of column metadata for the given key.
   *
   * Assumes that the min, max and mean are already calculated. gets the sigma
   *
   * @param key
   */
  public static void calculateSigma(final Key key, int col) {
    SigmaCalc sc = new SigmaCalc(key, col);
    sc.invoke(key);
    ValueArray va = ValueArray.value(key);
    va._cols[col]._sigma = sc.sigma();
    DKV.put(va._key, va.value());
    DKV.write_barrier();
  }

  static class SigmaCalc extends MRTask {

    public final Key _key;
    public int _col;
    public double _sigma; // std dev

    @Override
    public void map(Key key) {
      ValueArray va = ValueArray.value(_key);
      Column c = va._cols[_col];
      double mean = c._mean;
      AutoBuffer bits = va.getChunk(key);
      int num = bits.remaining() / va._rowsize;
      for( int i = 0; i < num; ++i ) {
        double x = va.datad(bits, i, c);
        _sigma += (x - mean) * (x - mean);
      }
    }

    @Override
    public void reduce(DRemoteTask drt) {
      SigmaCalc other = (SigmaCalc) drt;
      _sigma += other._sigma;
    }

    public SigmaCalc(Key key, int col) { // constructor
      _key = key;
      _col = col;
      _sigma = 0;
    }

    public double sigma() {
      ValueArray va = ValueArray.value(_key);
      return Math.sqrt(_sigma / va.numRows());
    }
  }

  // ---------------------------------------------------------------------------
  // Assignments

  /**
   * Assigns (copies) the what argument to the given key.
   *
   * TODO at the moment, only does deep copy.
   *
   * @param to
   * @param what
   * @throws EvaluationException
   */
  public static void assign(int pos, final Key to, Result what) throws EvaluationException {
    if( what._type == Result.Type.rtNumberLiteral ) { // assigning to a constant creates a vector of size 1
      // The 1 tiny arraylet
      Key key2 = ValueArray.getChunkKey(0, to);
      byte[] bits = new byte[8];
      UDP.set8d(bits, 0, what._const);
      Value val = new Value(key2, bits);
      Futures fs = new Futures();
      DKV.put(key2, val, fs);
      // The metadata
      VABuilder b = new VABuilder(to.toString(),1).addDoubleColumn("0",what._const, what._const, what._const,0).createAndStore(to);
      fs.blockForPending();
    } else if (what._type == Result.Type.rtKey) {
      if( what.canShallowCopy() ) {
        throw H2O.unimpl();
      } else if (what.rawColIndex()!=-1) { // copy in place of a single column only
        ValueArray v = ValueArray.value(what._key);
        if( v == null )
          throw new EvaluationException(pos, "Key " + what._key + " not found");
        int col = what.rawColIndex();
        Column c = v._cols[col];
        VABuilder b = new VABuilder(to.toString(), v.numRows()).addColumn(c._name,c._size, c._scale,c._min, c._max, c._mean, c._sigma).createAndStore(to);
        DeepSingleColumnAssignment da = new DeepSingleColumnAssignment(what._key, to, col);
        da.invoke(to);
      } else {
        ValueArray v = ValueArray.value(what._key);
        if( v == null )
          throw new EvaluationException(pos, "Key " + what._key + " not found");
        ValueArray r = v.clone();
        r._key = to;
        DKV.put(to, r.value());
        DKV.write_barrier();
        MRTask copyTask = new MRTask() {
          @Override public void map(Key fromk) {
            long chkidx = ValueArray.getChunkIndex(fromk);
            Key tok = ValueArray.getChunkKey(chkidx, to);
            byte[] bits = DKV.get(fromk).memOrLoad();
            Value tov = new Value(tok, MemoryManager.arrayCopyOf(bits, bits.length));
            DKV.put(tok, tov, getFutures());
          }
          @Override  public void reduce(DRemoteTask drt) { }
        };
        copyTask.invoke(what._key);
      }
    } else {
      throw new EvaluationException(pos,"Only Values and numeric constants can be assigned");
    }
  }

  // sigma ---------------------------------------------------------------------

  /** Creates a simple vector using the given values only.
   *
   * @param name
   * @param items
   */
  public void createVector(Key name, String colName, double[] items) {
    // TODO TODO TODO
    VABuilder b = new VABuilder(name.toString(), items.length).addDoubleColumn(colName);
    ValueArray va = b.create(name);
    byte[] bits = null;
    int offset = 0;
    double min = Double.MAX_VALUE;
    double max = -Double.MAX_VALUE;
    double tot = 0;
    for (int i = 0; i < items.length; ++i) {
      if ((bits == null) || (offset == bits.length)) { // create new chunk
        offset = 0;

      }
      UDP.set8d(bits,offset,items[i]);
      offset += 8;
      if (items[i] < min)
        min = items[i];
      if (items[i] > max)
        max = items[i];
      tot += items[i];
    }
    tot = tot / items.length;
    b.setColumnStats(0,min,max,tot);
    b.createAndStore(name);
  }

}


/** TODO scaling is missing, I should probably do VA iterators to do the job
 * for me much better.
 *
 * TODO!!!!!!!!!!!!
 *
 * @author peta
 */
class DeepSingleColumnAssignment extends MRTask {

  private Key _to;
  private Key _from;
  private int _colIndex;


  @Override public void map(Key key) {
    ValueArray vTo = ValueArray.value(_to);
    ValueArray vFrom = ValueArray.value(_from);
    int colSize = vFrom._cols[_colIndex]._size;
    assert colSize == vTo._cols[0]._size;
    long cidx = ValueArray.getChunkIndex(key);
    long row = vTo.startRow(cidx);
    long chunkRows = vTo.rpc(cidx);
    AutoBuffer bits = new AutoBuffer((int)chunkRows*vTo._rowsize);
    for (int i = 0; i < chunkRows; ++i) {
      switch (colSize) {
      case 1:  bits.put1 ((int  )vFrom.datad(i+row, _colIndex)); break;
      case 2:  bits.put2 ((char )vFrom.datad(i+row, _colIndex)); break;
      case 4:  bits.put4 ((int  )vFrom.datad(i+row, _colIndex)); break;
      case 8:  bits.put8 (       vFrom.data (i+row, _colIndex)); break;
      case -4: bits.put4f((float)vFrom.datad(i+row, _colIndex)); break;
      case -8: bits.put8d(       vFrom.datad(i+row, _colIndex)); break;
      default:
        throw new RuntimeException("Unsupported colSize "+colSize);
      }
    }
    // we have the bytes now, just store the value
    Value val = new Value(key, bits.buf());
    DKV.put(key, val, getFutures());
  }

  @Override public void reduce(DRemoteTask drt) { }

  public DeepSingleColumnAssignment(Key from, Key to, int colIndex) {
    _to = to;
    _from = from;
    _colIndex = colIndex;
  }
}

