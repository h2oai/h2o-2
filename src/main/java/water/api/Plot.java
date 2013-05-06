package water.api;

import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.*;
import java.util.Properties;

import javax.imageio.ImageIO;

import water.*;
import water.util.Log;

public class Plot extends Request {
  protected final H2OHexKey       _source   = new H2OHexKey(SOURCE_KEY);
  protected final H2OExistingKey  _clusters = new H2OExistingKey(CLUSTERS);
  protected final Int             _width    = new Int(WIDTH, 800);
  protected final Int             _height   = new Int(HEIGHT, 800);
  protected final HexColumnSelect _columns  = new HexColumnSelect(COLS, _source);
  protected final H2OKey          _dest     = new H2OKey(DEST_KEY, (Key) null);

  public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
    checkArguments(args, type);
    ValueArray va = _source.value();
    Key source = va._key;
    int width = _width.value();
    int height = _height.value();
    int[] cols = _columns.value();

    switch( type ) {
    case www: {
      String k = source.toString();
      //@formatter:off
      String s = ""
        + "<div class='container' style='margin: 0px auto'>"
        +   "<h3>" + k + "</h3>"
        +   "<img href='Plot.png?source=" + k + "'></img>"
        + "</div>";
      return wrap(server, s);
      //@formatter:on
    }
    case png: {
      byte[] pixels = hex.Plot.run(va, width, height, cols);

      BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
      WritableRaster raster = image.getRaster();
      int[] pixel = new int[3];
      for( int y = 0; y < height; y++ ) {
        for( int x = 0; x < width; x++ ) {
          int v = pixels[y * width + x] & 0xff;
          if( v != 0 ) // Make sure visible even with low count
            v = 128 +v / 2 ;
          pixel[0] = 255 - v;
          pixel[1] = 255 - v;
          pixel[2] = 255 - v;
          raster.setPixel(x, y, pixel);
        }
      }
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try {
        boolean res = ImageIO.write(image, "png", buffer);
        if( !res )
          throw new RuntimeException();
      } catch( IOException e ) {
        throw  Log.errRTExcept(e);
      }
      InputStream in = new ByteArrayInputStream(buffer.toByteArray());
      return server.new Response(NanoHTTPD.HTTP_OK, "image/png", in);
    }
    default:
      throw new RuntimeException();
    }
  }

  @Override
  protected Response serve() {
    throw new RuntimeException();
  }
}
