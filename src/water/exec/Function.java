
package water.exec;

import java.util.*;

import water.*;
import water.ValueArray.Column;
import water.exec.Expr.Result;

/** A class that represents the function call.
 *
 * Checks arguments in a proper manner using the argchecker instances and
 * executes the function. Subclasses should only override the doEval abstract
 * method.
 *
 * @author peta
 */

public abstract class Function {

  // ArgCheck ------------------------------------------------------------------
  public abstract class ArgCheck {
    public final String _name;
    public final Result _defaultValue;

    protected ArgCheck() {
      _name = null; // required
      _defaultValue = null;
    }

    protected ArgCheck(String name) {
      _name = name;
      _defaultValue = null;
    }

    protected ArgCheck(String name, double defaultValue) {
      _name = name;
      _defaultValue = Result.scalar(defaultValue);
    }

    protected ArgCheck(String name, String defaultValue) {
      _name = name;
      _defaultValue = Result.string(defaultValue);
    }

    public abstract void checkResult(Result r) throws Exception;
  }

  // ArgScalar -----------------------------------------------------------------
  public class ArgValue extends ArgCheck {
    public ArgValue() { }
    public ArgValue(String name) { super(name); }
    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtKey)
        throw new Exception("Expected value (possibly multiple columns)");
    }
  }


  // ArgScalar -----------------------------------------------------------------
  public class ArgScalar extends ArgCheck {
    public ArgScalar() { }
    public ArgScalar(String name) { super(name); }
    public ArgScalar(String name, double defaultValue) { super(name,defaultValue); }
    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtNumberLiteral)
        throw new Exception("Expected number literal");
    }
  }

  // ArgInt --------------------------------------------------------------------
  public class ArgInt extends ArgCheck {
    public ArgInt() { }
    public ArgInt(String name) { super(name); }
    public ArgInt(String name, long defaultValue) { super(name,defaultValue); }
    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtNumberLiteral)
        throw new Exception("Expected number");
      if ((long) r._const != r._const)
        throw new Exception("Expected integer number");
    }
  }

  // ArgIntPositive-------------------------------------------------------------
  public class ArgIntPositive extends ArgCheck {
    public ArgIntPositive() { }
    public ArgIntPositive(String name) { super(name); }
    public ArgIntPositive(String name, long defaultValue) { super(name,defaultValue); }
    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtNumberLiteral)
        throw new Exception("Expected number");
      if ((long) r._const != r._const)
        throw new Exception("Expected integer number");
      if (r._const < 0)
        throw new Exception("Expected positive argument");
    }
  }

  // ArgString -----------------------------------------------------------------
  public class ArgString extends ArgCheck {
    public ArgString() { }
    public ArgString(String name) { super(name); }
    public ArgString(String name, String defaultValue) { super(name,defaultValue); }
    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtStringLiteral)
        throw new Exception("Expected string literal");
    }
  }

  // ArgColIdent ---------------------------------------------------------------
  public class ArgColIdent extends ArgCheck {
    public ArgColIdent() { }
    public ArgColIdent(String name) { super(name); }
    public ArgColIdent(String name, String defaultValue) { super(name,defaultValue); }
    public ArgColIdent(String name, int defaultValue) { super(name,String.valueOf(defaultValue)); }

    @Override public void checkResult(Result r) throws Exception {
      if (r._type == Result.Type.rtStringLiteral)
        return;
      if (r._type == Result.Type.rtNumberLiteral)
        if (Math.ceil(r._const) == Math.floor(r._const))
          return;
      throw new Exception("String or integer expected.");
    }
  }

  // ArgSingleColumn -----------------------------------------------------------
  public class ArgVector extends ArgCheck {
    public ArgVector() { }
    public ArgVector(String name) { super(name); }

    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtKey)
        throw new Exception("Expected vector (value)");
      if (r.rawColIndex() >= 0) // that is we are selecting single column
        return;
      ValueArray va = ValueArray.value(DKV.get(r._key));
      if (va.numCols()!=1)
        throw new Exception("Expected single column vector, but "+va.numCols()+" columns found.");
    }
  }


  // Function implementation ---------------------------------------------------
  private ArrayList<ArgCheck> _argCheckers = new ArrayList();
  private HashMap<String,Integer> _argNames = new HashMap();
  public final String _name;

  protected void addChecker(ArgCheck checker) {
    if (checker._name!=null)
      _argNames.put(checker._name,_argCheckers.size());
    _argCheckers.add(checker);
  }

  public ArgCheck checker(int index) {
    return _argCheckers.get(index);
  }

  public int numArgs() {
    return _argCheckers.size();
  }

  public int argIndex(String name) {
    Integer i = _argNames.get(name);
    return i == null ? -1 : i;
  }

  public Function(String name) {
    _name = name;
    assert (FUNCTIONS.get(name) == null);
    FUNCTIONS.put(name,this);
  }


  public abstract Result eval(Result... args) throws Exception;

  // static list of all functions

  public static final HashMap<String,Function> FUNCTIONS = new HashMap();

  public static void initializeCommonFunctions() {
    new Min("min");
    new Max("max");
    new Sum("sum");
    new Mean("mean");
    new Filter("filter");
    new Slice("slice");
    new RandBitVect("randomBitVector");
    new RandomFilter("randomFilter");
    new Log("log");
    new InPlaceColSwap("colSwap");
    new MakeEnum("factor");
  }
}

// Min -------------------------------------------------------------------------

class Min extends Function {

  static class MRMin extends Helpers.ScallarCollector {

    @Override protected void collect(double x) { if (x < _result) _result = x; }

    @Override protected void reduce(double x) { if (x < _result) _result = x; }

    public MRMin(Key k, int col) { super(k,col,Double.MAX_VALUE); }
  }

  public Min(String name) {
    super(name);
    addChecker(new ArgVector("src"));
  }

  @Override public Result eval(Result... args) throws Exception {
    MRMin task = new MRMin(args[0]._key, args[0].rawColIndex());
    task.invoke(args[0]._key);
    return Result.scalar(task.result());
  }
}

// Max -------------------------------------------------------------------------

class Max extends Function {

  static class MRMax extends Helpers.ScallarCollector {

    @Override protected void collect(double x) { if (x > _result) _result = x; }

    @Override protected void reduce(double x) { if (x > _result) _result = x; }

    public MRMax(Key k, int col) { super(k,col,-Double.MAX_VALUE); }
  }

  public Max(String name) {
    super(name);
    addChecker(new ArgVector("src"));
  }

  @Override public Result eval(Result... args) throws Exception {
    MRMax task = new MRMax(args[0]._key, args[0].rawColIndex());
    task.invoke(args[0]._key);
    return Result.scalar(task.result());
  }
}

// Sum -------------------------------------------------------------------------

class Sum extends Function {

  static class MRSum extends Helpers.ScallarCollector {

    @Override protected void collect(double x) { _result += x; }

    @Override protected void reduce(double x) { _result += x; }

    public MRSum(Key k, int col) { super(k,col,0); }
  }

  public Sum(String name) {
    super(name);
    addChecker(new ArgVector("src"));
  }

  @Override public Result eval(Result... args) throws Exception {
    MRSum task = new MRSum(args[0]._key, args[0].rawColIndex());
    task.invoke(args[0]._key);
    return Result.scalar(task.result());
  }
}

// Mean ------------------------------------------------------------------------

class Mean extends Function {

  static class MRMean extends Helpers.ScallarCollector {

    @Override protected void collect(double x) { _result += x; }

    @Override protected void reduce(double x) { _result += x; }

    @Override public double result() {
      ValueArray va = ValueArray.value(_key);
      return _result / va.numRows();
    }

    public MRMean(Key k, int col) { super(k,col,0); }
  }

  public Mean(String name) {
    super(name);
    addChecker(new ArgVector("src"));
  }

  @Override public Result eval(Result... args) throws Exception {
    MRMean task = new MRMean(args[0]._key, args[0].rawColIndex());
    task.invoke(args[0]._key);
    return Result.scalar(task.result());
  }
}

// Filter ----------------------------------------------------------------------

class Filter extends Function {

  public Filter(String name) {
    super(name);
    addChecker(new ArgValue("src"));
    addChecker(new ArgVector("bitVect"));
  }

  @Override public Result eval(Result... args) throws Exception {
    Result r = Result.temporary();
    BooleanVectorFilter filter = new BooleanVectorFilter(r._key, args[0]._key, args[1]._key, args[1].colIndex());
    filter.invoke(args[0]._key);
    // Convert rows-per-chunk into start row# per chunk
    long numrows = 0;           // Filtered total row count
    for( int i=0; i<filter._rpc.length; i++ ) {
      long tmp = filter._rpc[i];
      filter._rpc[i] = numrows;
      numrows += tmp;
    }
    // Build target header
    ValueArray va = ValueArray.value(args[0]._key);
    va = VABuilder.updateRows(va, r._key, numrows);
    va._rpc = filter._rpc;      // Variable rows-per-chunk
    DKV.put(va._key, va.value());
    DKV.write_barrier();
    return r;
  }
}

// Slice -----------------------------------------------------------------------

class Slice extends Function {

  public Slice(String name) {
    super(name);
    addChecker(new ArgValue("src"));
    addChecker(new ArgIntPositive("start"));
    addChecker(new ArgIntPositive("count",-1));
  }

  @Override public Result eval(Result... args) throws Exception {
    // additional arg checking
    ValueArray ary = ValueArray.value(args[0]._key);
    long start = (long) args[1]._const;
    long length = (long) args[2]._const;
    if (start >= ary.numRows())
      throw new Exception("Start of the slice must be withtin the source data frame.");
    if (length == -1)
      length = ary.numRows() - start;
    if (start+length > ary.numRows())
      throw new Exception("Start + offset is out of bounds.");
    Result r = Result.temporary();
    ValueArray va = ValueArray.value(args[0]._key);
    va = VABuilder.updateRows(va, r._key, length);
    DKV.put(va._key, va.value());
    DKV.write_barrier();
    SliceFilter filter = new SliceFilter(args[0]._key,start,length);
    filter.invoke(r._key);
    assert (filter._filteredRows == length) : filter._filteredRows + " -- " + length;
    return r;
  }

}

// RandBitVect -----------------------------------------------------------------
class RandBitVect extends Function {
  static class RandVectBuilder extends MRTask {
    final Key _key;
    final long _selected;
    final long _numrows;
    final long _seed;
    long _createdSelected;

    public RandVectBuilder(Key k, long selected, long seed) {
      _key = k;
      _selected = selected;
      ValueArray va = ValueArray.value(k);
      _numrows = va._numrows;
      _seed = seed;
    }

    @Override public void map(Key key) {
      ValueArray va = ValueArray.value(_key);
      long cidx = ValueArray.getChunkIndex(key);
      int rows = va.rpc(cidx);
      long start = va.startRow(cidx);
      double ratio = (double)_selected/_numrows;
      int create = (int)(Math.round((start+rows)*ratio) - Math.round((start)*ratio));
      _createdSelected += create;
      // Fisher-Yates Shuffle
      byte[] bits = MemoryManager.malloc1(rows);
      for( int i = 0; i < create; ++i )
        bits[i] = 1;
      Random r = hex.rf.Utils.getDeterRNG(_seed+cidx);
      for( int i = rows-1; i >=1; --i) {
        int j = r.nextInt(i+1);
        byte x = bits[i];
        bits[i] = bits[j];
        bits[j] = x;
      }
      DKV.put(key, new Value(key,bits));
    }

    @Override  public void reduce(DRemoteTask drt) {
      RandVectBuilder other = (RandVectBuilder) drt;
      _createdSelected += other._createdSelected;
    }

  }

  public RandBitVect(String name) {
    super(name);
    addChecker(new ArgIntPositive("size"));
    addChecker(new ArgIntPositive("selected"));
    addChecker(new ArgInt("seed"));
  }

  @Override  public Result eval(Result... args) throws Exception {
    Result r = Result.temporary();
    long size = (long) args[0]._const;
    long selected = (long) args[1]._const;
    long seed = (long) args[2]._const;
    if (selected > size)
      throw new Exception("Number of selected rows must be smaller or equal than total number of rows for a random bit vector");
    double min = 0;
    double max = 1;
    double mean = selected / size;
    double var = Math.sqrt((1 - mean) * ( 1-mean) * selected + (mean*mean*(size-selected)) / size);
    new VABuilder("",size).addColumn("bits",1,1,min,max,mean,var).createAndStore(r._key);
    RandVectBuilder rvb = new RandVectBuilder(r._key,selected,seed);
    rvb.invoke(r._key);
    assert (rvb._createdSelected == selected) : rvb._createdSelected + " != " + selected;
    return r;
  }
}

// RandomFilter ----------------------------------------------------------------

class RandomFilter extends Function {

  public RandomFilter(String name) {
    super(name);
    addChecker(new ArgValue("src"));
    addChecker(new ArgIntPositive("rows"));
    addChecker(new ArgInt("seed"));
  }

  @Override public Result eval(Result... args) throws Exception {
    ValueArray ary = ValueArray.value(args[0]._key);
    long rows = (long) args[1]._const;
    if( rows < 0 || rows > ary.numRows())
      throw new Exception("Unable to sample more rows that are already present in the data frame");
    Result bVect = Function.FUNCTIONS.get("randomBitVector").eval(Result.scalar(ary.numRows()), args[1], args[2]);
    Result result = Function.FUNCTIONS.get("filter").eval(args[0],bVect);
    bVect.dispose();
    return result;
  }
}

// Log ------------------------------------------------------------------------
class Log extends Function {
  static class MRLog extends MRVectorUnaryOperator {
    public MRLog(Key key, Key result, int col) { super(key, result, col); }
    @Override public double operator(double opnd) {
      return Math.log(opnd);
    }
  }

  public Log(String name) {
    super(name);
    addChecker(new Function.ArgVector("src"));
  }

  @Override public Result eval(Result... args) throws Exception {
    Result r = Result.temporary();
    ValueArray va = ValueArray.value(args[0]._key);
    VABuilder b = new VABuilder("temp",va.numRows()).addDoubleColumn("0").createAndStore(r._key);
    MRLog task = new Log.MRLog(args[0]._key, r._key, args[0].colIndex());
    task.invoke(r._key);
    b.setColumnStats(0,task._min, task._max, task._tot / va.numRows()).createAndStore(r._key);
    return r;
  }
}

// makeEnum --------------------------------------------------------------------

/** Makes an enum from given class.
 *
 * @author peta
 */
class MakeEnum extends Function {

  static class GetEnumTask extends MRTask {
    water.parser.Enum _domain;
    final int _colIndex;
    final Key _aryKey;

    public GetEnumTask(Key aryKey, int colIndex) {
      _colIndex = colIndex;
      _aryKey = aryKey;
    }

    @Override public void init(){
      super.init();
      if (_domain == null)
        _domain = new water.parser.Enum();
    }

    @Override public void map(Key key) {
      ValueArray ary = ValueArray.value(_aryKey);
      AutoBuffer bits = ary.getChunk(key);
      Column c = ary._cols[_colIndex];
      final int rowsInChunk = ary.rpc(ValueArray.getChunkIndex(key));
      for (int i = 0; i < rowsInChunk; ++i)
        if( !ary.isNA(bits,i,c) )
          _domain.addKey(c._size < 0 // double or int string conversion?
                         ? String.valueOf(ary.datad(bits,i,c))   // double conversion
                         : String.valueOf(ary.data (bits,i,c))); // int conversion
    }

    @Override public void reduce(DRemoteTask drt) {
      GetEnumTask other = (GetEnumTask) drt;
      if (_domain == null) {
        _domain = other._domain;
      } else if (_domain != other._domain) {
        _domain.merge(other._domain);
      }
    }
  }

  static class PackToEnumTask extends MRTask {
    final water.parser.Enum _domain;
    final Key _resultKey;
    final Key _sourceKey;
    final int _sourceCol;

    double _tot;

    public PackToEnumTask(Key resultKey, Key sourceKey, int sourceCol, water.parser.Enum domain) {
      _domain = domain;
      _resultKey = resultKey;
      _sourceKey = sourceKey;
      _sourceCol = sourceCol;
    }

    @Override public void map(Key key) {
      ValueArray result = ValueArray.value(_resultKey);
      long cidx = ValueArray.getChunkIndex(key);
      long rowOffset = result.startRow(cidx);
      VAIterator source = new VAIterator(_sourceKey,_sourceCol, rowOffset);
      int size = _domain.size() < 255 ? 1 : 2;
      int chunkRows = result.rpc(cidx);
      AutoBuffer bits = new AutoBuffer(chunkRows * size);
      for( int i = 0; i < chunkRows; i++ ) {
        source.next();
        int id;
        if( source.isNA() ) {
          id = -1;              // Default miss value for enums
        } else {
          String s = source.defaultColumn()._size < 0 // double or int string conversion?
            ? String.valueOf(source.datad())          // double conversion
            : String.valueOf(source.data ());         // int conversion
          id = _domain.getTokenId(s);
          assert 0 <= id && id < _domain.size(); // we do not expect any misses here
          _tot += id;
        }
        if( size == 1 ) bits.put1(       id);
        else            bits.put2((short)id);
      }
      Value val = new Value(key, bits.bufClose());
      DKV.put(key, val, getFutures());
    }

    @Override
    public void reduce(DRemoteTask drt) {
      PackToEnumTask other = (PackToEnumTask) drt;
      _tot += other._tot;
    }
  }



  public MakeEnum(String name) {
    super(name);
    addChecker(new ArgVector("col"));
  }

  @Override
  public Result eval(Result... args) throws Exception {
    try {
      ValueArray oldAry = ValueArray.value(args[0]._key);
      // calculate the enums for the new encoded column
      GetEnumTask etask = new GetEnumTask(args[0]._key, args[0].colIndex());
      etask.invoke(args[0]._key);
      // error if we have too many of them
      if (etask._domain.isKilled())
        throw new Exception("More than 65535 unique values found. The column is too big for enums.");
      // compute the domain and determine the column properties
      Column oldCol = oldAry._cols[args[0].colIndex()];
      Column c = new Column();
      String[] domainStr = etask._domain.computeColumnDomain();
      c._domain = domainStr;
      c._base = 0;
      c._max = domainStr.length-1;
      c._min = 0;
      c._mean = Double.NaN;
      c._scale = 1;
      c._sigma = Double.NaN;
      c._size = (domainStr.length < 255) ? (byte)1 : (byte)2;
      c._name = oldCol._name;
      c._off = 0;
      c._n = oldCol._n;
      // create the temporary result and VA
      Result result = Result.temporary();
      ValueArray ary = new ValueArray(result._key, oldAry.numRows(), c._size, new Column[] { c });
      DKV.put(result._key, ary.value());
      DKV.write_barrier();
      // invoke the pack task
      PackToEnumTask ptask = new PackToEnumTask(result._key, args[0]._key, args[0].colIndex(),etask._domain);
      ptask.invoke(result._key);
      // update the mean
      c._mean = ptask._tot / c._n;
      ary = new ValueArray(result._key, oldAry.numRows(), c._size, new Column[] { c });
      DKV.put(result._key, ary.value());
      DKV.write_barrier();
      return result;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
}


// colSwap ---------------------------------------------------------------------

/** Swaps the column in place for a different column.
 *
 * @author peta
 */
class InPlaceColSwap extends Function {

  static class ColSwapTask extends MRTask {

    final Key _resultKey;
    final Key _oldKey;
    final Key _newKey;
    final int _oldCol;
    final int _newCol;

    public ColSwapTask(Key resultKey, Key oldKey, Key newKey, int oldCol, int newCol) {
      _resultKey = resultKey;
      _oldKey = oldKey;
      _newKey = newKey;
      _oldCol = oldCol;
      _newCol = newCol;
    }

    @Override public void map(Key key) {
      // a simple MR, get the row offset for the given key, then initialize the
      // iterators and patch the result
      ValueArray result = ValueArray.value(_resultKey);
      long cidx = ValueArray.getChunkIndex(key);
      int rowSize = result._rowsize;
      long rowOffset = result.startRow(cidx);
      VAIterator oldVal = new VAIterator(_oldKey,_oldCol, rowOffset);
      VAIterator newVal = new VAIterator(_newKey,_newCol, rowOffset);
      int chunkRows = result.rpc(cidx);
      AutoBuffer bits = new AutoBuffer(chunkRows*rowSize);
      // calculate the markers
      Column oldCol = oldVal._ary._cols[_oldCol];
      Column newCol = newVal._ary._cols[_newCol];
      int oldMark1 = oldCol._off;
      int newMark1 = newCol._off;
      int oldMark2 = Math.abs(oldCol._size) + oldMark1;
      int newMark2 = Math.abs(newCol._size) + newMark1;
      int oldMark3 = oldVal._ary._rowsize;
      for (int off = 0; off < bits.limit(); /* done in the body */) {
        oldVal.next();
        newVal.next();
        // copy & patch the data
        off = oldVal.copyCurrentRowPart(bits, off, 0, oldMark1);
        off = newVal.copyCurrentRowPart(bits, off, newMark1, newMark2);
        off = oldVal.copyCurrentRowPart(bits, off, oldMark2, oldMark3);
        assert (off % rowSize == 0);
      }
      // store the value
      Value val = new Value(key, bits.buf());
      DKV.put(key, val, getFutures());
    }

    @Override public void reduce(DRemoteTask drt) {
      // pass
    }
  }

  public InPlaceColSwap(String name) {
    super(name);
    addChecker(new ArgValue("source"));
    addChecker(new ArgColIdent("col"));
    addChecker(new ArgVector("newCol"));
  }


  @Override public Result eval(Result... args) throws Exception {
    // get and check the arguments
    Key oldKey = args[0]._key;
    Key newKey = args[2]._key;
    ValueArray oldAry = ValueArray.value(oldKey);
    ValueArray newAry = ValueArray.value(newKey);
    assert (oldAry != null);
    assert (newAry != null);
    int oldCol = Helpers.checkedColumnIndex(oldAry, args[1]);
    if (oldCol == -1)
      throw new Exception("Column not found in source value.");
    int newCol = args[2].colIndex();
    // calculate the new column headers for the result
    Column[] cols = new Column[oldAry.numCols()];
    int off = 0;
    for (int i = 0; i < cols.length; ++i) {
      if (oldCol == i) {
        cols[i] = newAry._cols[newCol];
        cols[i]._name = oldAry._cols[i]._name;
      } else {
        cols[i] = oldAry._cols[i];
      }
      cols[i]._off = (char) off;
      off += Math.abs(cols[i]._size);
    }
    // get the temporary result key
    Result result = Result.temporary();
    // we now have the new column layout and must do the copying, create the
    // value array
    ValueArray ary = new ValueArray(result._key, oldAry.numRows(), off, cols);
    DKV.put(result._key, ary.value());
    DKV.write_barrier();
    ColSwapTask task = new ColSwapTask(result._key, oldKey, newKey, oldCol, newCol);
    task.invoke(result._key);
    return result;
  }
}


// GLM -------------------------------------------------------------------------

class GLM extends Function {

  public GLM(String name) {
    super(name);
    addChecker(new ArgValue("key"));
    addChecker(new ArgColIdent("Y"));
    // no support for X
    // no support for negX
    addChecker(new ArgString("family","gaussian"));
    addChecker(new ArgScalar("xval",0));
    addChecker(new ArgScalar("threshold",0.5));
    addChecker(new ArgString("norm","NONE"));
    addChecker(new ArgScalar("lambda",0.1));
    addChecker(new ArgScalar("rho",1.0));
    addChecker(new ArgScalar("alpha",1.0));
  }

  @Override public Result eval(Result... args) throws Exception {


    throw new Exception("not implemented yet!");
  }


}