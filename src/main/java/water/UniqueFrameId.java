package water;

import dontweave.gson.JsonObject;
import dontweave.gson.JsonParser;
import org.apache.commons.codec.binary.Hex;

import water.api.DocGen;
import water.api.Request.API;
import water.api.Request.Default;
import water.fvec.Frame;

/**
 * Frames are mutable, so we can't create a unique id at creation time to distinguish
 * between Frames.  In addition, we'll want to know that two Frames parsed from the same
 * data are equivalent.  Therefore, we store the Frame here, and generate a good hash of
 * the contents of the Vecs when we are asked for the id.
 */

public final class UniqueFrameId extends UniqueId {
  private Key frame = null;

  public UniqueFrameId(Key key, Frame frame) {
    super(key);
    this.frame = frame._key;
  }

  public UniqueFrameId(String key, long creation_epoch_time_millis, String id, Frame frame) {
    super(key, creation_epoch_time_millis, id);
    this.frame = frame._key;
  }

  @Override
  public String getId() {
    return Long.toHexString(((Frame)DKV.get(frame).get()).checksum());
  }

  public JsonObject toJSON() {
    final String json = new String(writeJSON(new AutoBuffer()).buf());
    if (json.length() == 0) return new JsonObject();

    JsonObject jo = (JsonObject)new JsonParser().parse(json);
    jo.addProperty("id", this.getId());
    return jo;
  }
}
