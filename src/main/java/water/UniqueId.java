package water;

import water.api.DocGen;
import water.api.Request.API;
import water.api.Request.Default;

import java.util.UUID;

/**
 * Some properties to mix in to Frame, Model and such to make them uniquely identifyable.
 * That is, we want to distinguish between different instances of a Model that have the
 * same key over time.
 */

public final class UniqueId extends Iced {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;

  @API(help="The keycreation timestamp for the model (if it's not null).", required=false, filter=Default.class, json=true)
  private String key = null;

  @API(help="The creation timestamp for the model.", required=false, filter=Default.class, json=true)
  private long creation_epoch_time_millis = -1L;

  @API(help="The uuid for the model.", required=false, filter=Default.class, json=true)
  private String uuid = null;


  public UniqueId(Key key) {
    if (null != key)
      this.key = key.toString();
    this.creation_epoch_time_millis = System.currentTimeMillis();
    this.uuid = UUID.randomUUID().toString();
  }

  /**
   * ONLY to be used to deserializing persisted instances.
   */
  public UniqueId(String key, long creation_epoch_time_millis, String uuid) {
    this.key = key;
    this.creation_epoch_time_millis = creation_epoch_time_millis;
    this.uuid = uuid;
  }

  public String getKey() {
    return this.key;
  }

  public long getCreationEpochTimeMillis() {
    return this.creation_epoch_time_millis;
  }

  public String getUuid() {
    return this.uuid;
  }

  public boolean equals(Object o) {
    if (!(o instanceof UniqueId))
      return false;

    UniqueId other = (UniqueId)o;

    return
      (this.creation_epoch_time_millis == other.creation_epoch_time_millis) &&
      (this.uuid != null) &&
      (this.uuid.equals(other.uuid));
  }

  public int hashCode() {
    return 17 +
      37 * Long.valueOf(this.creation_epoch_time_millis).hashCode() +
      37 * this.uuid.hashCode();
  }
}
