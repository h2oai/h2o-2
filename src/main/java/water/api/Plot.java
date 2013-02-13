package water.api;

import jsr166y.RecursiveAction;
import water.*;

import com.google.gson.JsonObject;

public class Plot extends Request {

  protected final H2OHexKey       _source   = new H2OHexKey(SOURCE_KEY);
  protected final H2OExistingKey  _clusters = new H2OExistingKey(CLUSTERS);
  protected final Int             _width    = new Int(WIDTH, 800);
  protected final Int             _height   = new Int(HEIGHT, 600);
  protected final HexColumnSelect _columns  = new HexColumnSelect(COLS, _source);
  protected final H2OKey          _dest     = new H2OKey(DEST_KEY, (Key) null);

  @Override
  protected Response serve() {
    final ValueArray va = _source.value();
    final Key source = va._key;
    final int width = _width.value();
    final int height = _height.value();
    final int[] cols = _columns.value();
    Key dest = _dest.value();

    if( dest == null ) {
      String n = source.toString();
      int dot = n.lastIndexOf('.');

      if( dot > 0 )
        n = n.substring(0, dot);

      dest = Key.make(n + ".plot");
    }

    try {
      final Key dest_ = dest;
      UKV.put(dest, new hex.KMeans.KMeansModel());

      H2O.FJP_NORM.submit(new RecursiveAction() {
        @Override
        protected void compute() {
          hex.Plot.run(dest_, va, width, height, cols);
        }
      });

      JsonObject response = new JsonObject();
      response.addProperty(RequestStatics.DEST_KEY, dest.toString());

      Response r = PlotProgress.redirect(response, dest);
      r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
      return r;
    } catch( IllegalArgumentException e ) {
      return Response.error(e.getMessage());
    } catch( Error e ) {
      return Response.error(e.getMessage());
    }
  }
}
