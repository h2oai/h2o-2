package water.api;

import hex.Layer;
import hex.NeuralNet.NeuralNetModel;

import java.io.InputStreamReader;
import java.lang.reflect.Type;

import water.*;

import com.google.gson.*;

public class ImportModel extends Request2 {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;
  static final String DOC_GET = "Import a model from JSON";

  @API(help = "The key to import to", required = true, filter = Default.class)
  public Key destination_key;

  @API(help = "The type of model to import", required = true, filter = Default.class)
  public String type;

  @API(help = "JSON data", required = true, filter = Default.class)
  public ValueArray json;

  @Override protected Response serve() {
    GsonBuilder gb = new GsonBuilder();
    gb.registerTypeAdapter(Key.class, new JsonDeserializer() {
      @Override public Object deserialize(JsonElement json, Type type, JsonDeserializationContext context)
          throws JsonParseException {
        assert type == Key.class;
        return Key.make(json.getAsString());
      }
    });
    gb.registerTypeAdapter(Layer.class, new JsonDeserializer() {
      @Override public Object deserialize(JsonElement json, Type type, JsonDeserializationContext context)
          throws JsonParseException {
        assert type == Layer.class;
        try {
          Class c = Class.forName(((JsonObject) json).get("type").getAsString());
          return context.deserialize(json, c);
        } catch( Throwable e ) {
          throw new RuntimeException(e);
        }
      }
    });
    Gson gson = gb.create();
    Model model;
    if( NeuralNetModel.class.getSimpleName().equals(type) )
      model = gson.fromJson(new InputStreamReader(json.openStream()), NeuralNetModel.class);
    else
      throw new UnsupportedOperationException("Import of " + type + " is not yet supported.");
    UKV.put(destination_key, model);
    return Response.done(this);
  }
}
