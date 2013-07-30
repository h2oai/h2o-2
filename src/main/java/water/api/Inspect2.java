package water.api;

import water.*;
import water.Weaver.Weave;
import water.fvec.Frame;

public class Inspect2 extends Request {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Inspect a fluid-vec frame";

  @Weave(help="An existing H2O Frame key.")
  final FrameKey src_key = new FrameKey("src_key");

  public static Response redirect(Request req, String src_key) {
    return new Response(Response.Status.redirect, req, -1, -1, "Inspect2", "src_key", src_key );
  }

  @Override protected Response serve() {
    Frame fr = src_key.value();
    if( fr == null ) return RequestServer._http404.serve();
    return new Response(Response.Status.done, this, -1, -1, null);
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    Frame fr = src_key.value();
    throw H2O.unimpl();
  }

  public class FrameKey extends TypeaheadInputText<Frame> {
    public FrameKey(String name) { super(TypeaheadHexKeyRequest.class, name, true); }

    @Override protected Frame parse(String input) throws IllegalArgumentException {
      Key k = Key.make(input);
      Value v = DKV.get(k);
      if (v == null)    throw new IllegalArgumentException("Key "+input+" not found!");
      Iced ice = v.get();
      if( !(ice instanceof Frame) ) throw new IllegalArgumentException("Key "+input+" is not a valid Frame key");
      return v.get();
    }
    @Override protected Frame defaultValue() { return null; }
    @Override protected String queryDescription() { return "An existing H2O Frame key."; }
  }
}
