package water.api;

import com.google.gson.JsonObject;

public class NeuralNet extends Request {
  @Override protected Response serve() {
    try {
      JsonObject response = new JsonObject();
      Response r = Response.done(response);
      r.setBuilder(DEST_KEY, new KeyElementBuilder());
      return r;
    } catch( IllegalArgumentException e ) {
      return Response.error(e.getMessage());
    } catch( Error e ) {
      return Response.error(e.getMessage());
    }
  }
}
