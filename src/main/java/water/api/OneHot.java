package water.api;

import water.*;
import water.fvec.Frame;
import water.util.Log;
import water.util.RString;

public class OneHot extends Request2 {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    @API(help = "Data frame", required = true, filter = Default.class)
    public Frame source;

    @API(help = "Destination key", required = false, filter = Default.class)
    protected final Key destination_key = Key.make("__OneHot_" + Key.make());

    @API(help = "Ignored columns by name and zero-based index", filter=colsNamesIdxFilter.class, displayName="Ignored columns")
    public int[] ignored_cols = new int[0];
    class colsNamesIdxFilter extends MultiVecSelect { public colsNamesIdxFilter() {super("source", MultiVecSelectType.NAMES_THEN_INDEXES); } }


//    public static String link(Key k, String content) {
//        RString rs = new RString("<a href='OneHot.html?source=%$key'>%content</a>");
//        rs.replace("key", k.toString());
//        rs.replace("content", content);
//        return rs.toString();
//    }

    @Override protected Response serve() {
        try {
          Frame fr = new Frame(destination_key,source._names.clone(),source.vecs().clone()).delete_and_lock(null);
          fr.remove(ignored_cols);
          Frame oneHotFrame = hex.OneHot.expandDataset(fr,destination_key);
          for (int i : ignored_cols) oneHotFrame.add(source._names[i], source.vecs()[i]);
          oneHotFrame.unlock(null);
        } catch( Throwable t ) {
          return Response.error(t);
        }
        return Inspect2.redirect(this, destination_key.toString());
    }
}
