package water.api;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.persist.PersistS3Task;
import water.persist.PersistS3;
import water.util.Log;

import com.google.gson.JsonObject;

public class ExportS3 extends Request {

  protected class BucketArg extends TypeaheadInputText<String> {
    public BucketArg(String name) {
      super(TypeaheadS3BucketRequest.class, name, true);
    }

    @Override
    protected String parse(String input) throws IllegalArgumentException {
      PersistS3.getClient();
      return input;
    }

    @Override
    protected String queryDescription() {
      return "S3 Bucket";
    }

    @Override
    protected String defaultValue() {
      return null;
    }
  }

  protected final H2OExistingKey _source = new H2OExistingKey(SOURCE_KEY);
  protected final BucketArg      _bucket = new BucketArg(BUCKET);
  protected final Str            _object = new Str(OBJECT, "");

  public ExportS3() {
    _requestHelp = "Exports a key to Amazon S3.  All nodes in the "
        + "cloud must have permission to access the Amazon object.";
    _bucket._requestHelp = "Target Amazon S3 Bucket.";
  }

  @Override
  protected Response serve() {
    final Value value = _source.value();
    final String bucket = _bucket.value();
    final String object = _object.value();

    try {
      final Key dest = PersistS3Task.init(value);
      H2O.submitTask(new H2OCountedCompleter() {
        @Override public void compute2() {
          PersistS3Task.run(dest, value, bucket, object);
        }
      });

      JsonObject response = new JsonObject();
      response.addProperty(RequestStatics.DEST_KEY, dest.toString());

      Response r = ExportS3Progress.redirect(response, dest);
      r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
      return r;
    } catch( Throwable e ) {
      return Response.error(e);
    }
  }
}
