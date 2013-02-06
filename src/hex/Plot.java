package hex;

import water.*;
import water.ValueArray.Column;

/**
 * Basic data visualization using map/reduce.
 */
public abstract class Plot {
  private static final boolean DEBUG = false;

  public static class Res extends Iced {
    public byte[] _pixels;
  }

  static public void run(Key dest, ValueArray va, int width, int height, int... cols) {
    final Res res = new Res();

    // TODO PCA if more than two columns

    // Count dots falling in each pixel
    Pixels task = new Pixels();
    task._arykey = va._key;
    task._width = width;
    task._height = height;
    task._cols = cols;
    task.invoke(va._key);
    res._pixels = task._pixels;
    UKV.put(dest, res);
  }

  static class Pixels extends MRTask {
    Key    _arykey;
    int    _width, _height;
    int[]  _cols;

    // Reduced
    byte[] _pixels;

    @Override
    public void map(Key key) {
      if( DEBUG )
        System.out.println("pixels map " + key + ": " + this);

      assert key.home();
      ValueArray va = ValueArray.value(DKV.get(_arykey));
      AutoBuffer bits = va.getChunk(key);
      int rows = bits.remaining() / va._rowsize;
      _pixels = new byte[_height * _width];

      for( int row = 0; row < rows; row++ ) {
        Column cX = va._cols[_cols[0]];
        Column cY = va._cols[_cols[1]];
        double x = va.datad(bits, row, cX);
        double y = va.datad(bits, row, cY);
        x = (x - cX._min) / (cX._max - cX._min);
        y = (y - cY._min) / (cY._max - cY._min);
        int iX = (int) (x * (_width - 1));
        int iY = (int) (y * (_height - 1));
        int value = _pixels[iY * _width + iX] & 0xff;
        value = value == 0xff ? value : value + 1;
        _pixels[iY * _width + iX] = (byte) value;
      }
    }

    @Override
    public void reduce(DRemoteTask rt) {
      if( DEBUG )
        System.out.println("pixels reduce " + this);

      Pixels task = (Pixels) rt;

      if( _pixels == null )
        _pixels = task._pixels;
      else {
        for( int i = 0; i < _pixels.length; i++ ) {
          int value = _pixels[i] & 0xff + task._pixels[i] & 0xff;
          _pixels[i] = (byte) Math.min(0xff, value);
        }
      }
    }
  }
}
