package hex;

import water.*;
import water.ValueArray.Column;

/**
 * Basic data visualization using map/reduce.
 */
public abstract class Plot {

  public static byte[] run(ValueArray va, int width, int height, int... cols) {
    // TODO PCA if more than two columns

    // Count dots falling in each pixel
    Pixels task = new Pixels();
    task._arykey = va._key;
    task._width = width;
    task._height = height;
    task._cols = cols;
    task.invoke(va._key);
    return task._pixels;
  }

  public static int scale(double d, Column c, int screen) {
    d = (d - c._min) / (c._max - c._min);
    return (int) (d * (screen - 1));
  }

  static class Pixels extends MRTask {
    Key _arykey;
    int _width, _height;
    int[] _cols;

    // Reduced
    byte[] _pixels;

    @Override public void map(Key key) {
      assert key.home();
      ValueArray va = DKV.get(_arykey).get();
      AutoBuffer bits = va.getChunk(key);
      int rows = bits.remaining() / va._rowsize;
      _pixels = new byte[_height * _width];

      for( int row = 0; row < rows; row++ ) {
        Column cX = va._cols[_cols[0]];
        Column cY = va._cols[_cols[1]];
        double x = va.datad(bits, row, cX);
        double y = va.datad(bits, row, cY);
        int iX = scale(x, cX, _width);
        int iY = scale(y, cY, _height);
        if( iX < 0 ) iX = 0; // Bound for numeric instability
        if( iX >= _width ) iX = _width - 1;
        if( iY < 0 ) iX = 0; // Bound for numeric instability
        if( iY >= _height ) iY = _height - 1;
        int value = _pixels[iY * _width + iX] & 0xff;
        value = value == 0xff ? value : value + 1;
        dot(iX, iY, value);
      }
    }

    private void dot(int iX, int iY, int value) {
      int r = 2;
      for( int y = Math.max(0, iY - r); y < Math.min(_height, iY + r); y++ )
        for( int x = Math.max(0, iX - r); x < Math.min(_width, iX + r); x++ )
          _pixels[y * _width + x] = (byte) value;
    }

    @Override public void reduce(DRemoteTask rt) {
      Pixels task = (Pixels) rt;

      if( _pixels == null ) _pixels = task._pixels;
      else {
        for( int i = 0; i < _pixels.length; i++ ) {
          int value = _pixels[i] & 0xff + task._pixels[i] & 0xff;
          _pixels[i] = (byte) Math.min(0xff, value);
        }
      }
    }
  }
}
