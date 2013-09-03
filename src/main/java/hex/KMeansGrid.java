package hex;

import java.util.UUID;

import water.*;
import water.api.DocGen;
import water.fvec.*;
import water.util.RString;
import water.util.Utils;

public class KMeansGrid extends KMeansShared {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Grid search for k-means parameters";

//@formatter:off
  @API(help = "Number of clusters", required = true, filter = kFilter.class)
  public int[] k;
  class kFilter extends RSeq { public kFilter() { super("2:10:1", false); } }

  @API(help = "Maximum number of iterations", required = true, filter = max_iterFilter.class)
  public int[] max_iter;
  class max_iterFilter extends RSeq { public max_iterFilter() { super("10:100:10", true); } }

  @API(help = "Columns to use as input")
  public int[] cols;
  class colsFilter extends ColumnSelect { public colsFilter() { super("source_key"); } }
//@formatter:on

  public KMeansGrid() {
    description = DOC_GET;
  }

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='KMeansGrid.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", SOURCE_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected void onArgumentsParsed() {
    if( source_key != null && destination_key == null ) {
      String n = source_key.toString();
      int dot = n.lastIndexOf('.');
      if( dot > 0 )
        n = n.substring(0, dot);
      String res = n + Extensions.KMEANS_GRID;
      destination_key = Key.make(res);
    }
  }

  @Override protected void run() {
    ValueArray va = DKV.get(source_key).get();
    Key temp = null;
    try {
      temp = Key.make(UUID.randomUUID().toString(), (byte) 1, Key.DFJ_INTERNAL_USER);
      hex.KMeans first = hex.KMeans.start(temp, va, k[0], initialization, max_iter[0], seed, normalize, cols);
      KMeansModel model = first.get();

      String[] names = new String[3];
      Vec[] vecs = new Vec[names.length];
      NewChunk[] chunks = new NewChunk[names.length];
      for( int c = 0; c < chunks.length; c++ ) {
        vecs[c] = new AppendableVec(UUID.randomUUID().toString());
        chunks[c] = new NewChunk(vecs[c], 0);
      }
      names[0] = "k";
      names[1] = "max_iter";
      names[2] = "error";
      for( int ki = 0; ki < k.length; ki++ ) {
        for( int mi = 0; mi < max_iter.length; mi++ ) {
          if( ki != 0 || mi != 0 ) {
            KMeans job = KMeans.start(first.dest(), va, k[ki], initialization, max_iter[mi], seed, normalize, cols);
            model = job.get();
          }
          chunks[0].addNum(k[ki]);
          chunks[1].addNum(max_iter[mi]);
          chunks[2].addNum(model._error);
        }
      }
      for( int c = 0; c < vecs.length; c++ ) {
        chunks[c].close(0, null);
        vecs[c] = ((AppendableVec) vecs[c]).close(null);
      }
      UKV.put(destination_key, new Frame(names, vecs));
    } finally {
      UKV.remove(temp);
    }
    remove();
  }
}