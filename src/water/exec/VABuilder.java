package water.exec;
import java.util.ArrayList;

import water.*;
import water.ValueArray.Column;

/** A simple class that automates construction of ValueArrays.
 *
 * @author peta
 */
public class VABuilder {
  private long _numRows;
  private byte _persistence;
  private String _name;
  private ArrayList<Column> _cols = new ArrayList();


  public VABuilder(String name,long numRows) {
    _numRows = numRows;
    _persistence = Value.ICE;
    _name = name;
  }


  public VABuilder addDoubleColumn(String name) {
    Column c = new Column();
    c._name = name == null ? new String() : name;
    c._size = -8;
    c._scale = 1;
    c._min = Double.NaN;
    c._max = Double.NaN;
    c._mean = Double.NaN;
    c._sigma = Double.NaN;
    c._domain = null;
    _cols.add(c);
    return this;
  }

  public VABuilder addDoubleColumn(String name, double min, double max, double mean) {
    Column c = new Column();
    c._name = name == null ? new String() : name;
    c._size = -8;
    c._scale = 1;
    c._min = min;
    c._max = max;
    c._mean = mean;
    c._sigma = Double.NaN;
    c._domain = null;
    _cols.add(c);
    return this;
  }

  public VABuilder addDoubleColumn(String name, double min, double max, double mean, double sigma) {
    Column c = new Column();
    c._name = name == null ? new String() : name;
    c._size = -8;
    c._scale = 1;
    c._min = min;
    c._max = max;
    c._mean = mean;
    c._sigma = sigma;
    c._domain = null;
    _cols.add(c);
    return this;
  }

  public VABuilder addColumn(String name, int size, int scale, double min, double max, double mean, double sigma) {
    Column c = new Column();
    c._name = name == null ? new String() : name;
    c._size = (byte)size;
    c._scale = (char)scale;
    c._min = min;
    c._max = max;
    c._mean = mean;
    c._sigma = sigma;
    c._domain = null;
    _cols.add(c);
    return this;
  }

  public VABuilder addColumn(Column other) {
    Column c = new Column();
    c._name = other._name;
    c._size = other._size;
    c._scale = other._scale;
    c._min = other._min;
    c._max = other._max;
    c._mean = other._mean;
    c._sigma = other._sigma;
    c._domain = null;
    _cols.add(c);
    return this;
  }

  public VABuilder setColumnStats(int colIndex, double min, double max, double mean) {
    Column c = _cols.get(colIndex);
    c._min = min;
    c._max = max;
    c._mean = mean;
    return this;
  }

  public VABuilder setColumnSigma(int colIndex, double sigma) {
    Column c = _cols.get(colIndex);
    c._sigma = sigma;
    return this;
  }

  public ValueArray create(Key k) {
    Column[] cols = _cols.toArray(new Column[_cols.size()]);
    int rowSize = 0;
    for (Column c: cols)
      rowSize += Math.abs(c._size);
    return new ValueArray(k, _numRows, rowSize, cols);
  }

  public VABuilder createAndStore(Key k) {
    ValueArray v = create(k);
    Futures fs = new Futures();
    DKV.put(k, v.value(), fs);
    fs.blockForPending();
    return this;
  }


  public static ValueArray updateRows(ValueArray old, Key newKey, long newRows) {
    ValueArray newAry = old.clone();
    newAry._key = newKey;
    newAry._numrows = newRows;
    return newAry;
  }
}
