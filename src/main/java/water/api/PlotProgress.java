package water.api;

import water.Key;
import water.Value;

import com.google.gson.*;

public class PlotProgress extends Request {
  protected final H2OExistingKey _dest = new H2OExistingKey(DEST_KEY);

  public static Response redirect(JsonObject fromPageResponse, Key dest) {
    JsonObject destPageParams = new JsonObject();
    destPageParams.addProperty(DEST_KEY, dest.toString());
    return Response.redirect(fromPageResponse, PlotProgress.class, destPageParams);
  }

  @Override
  protected Response serve() {
    Value v = _dest.value();
    hex.KMeans.KMeansModel res = v.get(new hex.KMeans.KMeansModel());
    JsonObject response = new JsonObject();
    response.addProperty(RequestStatics.DEST_KEY, v._key.toString());

    if( res._clusters == null ) {
      // Keeps moving
      return Response.poll(response, 1 - 1 / (res._iteration + 1));
    }

    response.addProperty(NUM_ROWS, res._clusters.length);
    JsonArray arr = new JsonArray();

    for( double[] cluster : res._clusters ) {
      JsonArray row = new JsonArray();
      arr.add(row);

      for( double d : cluster )
        row.add(new JsonPrimitive(d));
    }

    response.add(ROWS, arr);

    Response r = Response.done(response);
    r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
    return r;
  }
}
