package hex;

import hex.gbm.GBM;
import hex.gbm.GBM.GBMModel;

import java.util.*;

import water.*;
import water.Job.FrameJob;
import water.api.*;
import water.fvec.*;
import water.util.RString;

public class GBMGrid extends FrameJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Grid search for GBM";
  static int KEYS_INDEX = 7;

//@formatter:off
  @API(help="", required=true, filter=GBMVecSelect.class)
  public Vec vresponse;
  class GBMVecSelect extends VecClassSelect { GBMVecSelect() { super("source"); } }

  @API(help="columns to ignore", required=false, filter=GBMMultiVecSelect.class)
  public int[] ignored_cols = new int[0];
  class GBMMultiVecSelect extends MultiVecSelect { GBMMultiVecSelect() { super("source");} }

  //

  @API(help = "Number of trees", filter=ntreesFilter.class, lmin=1, lmax=1000000)
  public int[] ntrees;
  class ntreesFilter extends RSeq { public ntreesFilter() { super("10,100", false); } }

  @API(help = "Maximum tree depth", filter=max_depthFilter.class, lmin=0, lmax=10000)
  public int[] max_depth;
  class max_depthFilter extends RSeq { public max_depthFilter() { super("1,5,10", false); } }

  @API(help = "Fewest allowed observations in a leaf", filter=min_rowsFilter.class, lmin=1)
  public int[] min_rows;
  class min_rowsFilter extends RSeq { public min_rowsFilter() { super("10", false); } }

  @API(help = "Build a histogram of this many bins, then split at the best point", filter=nbinsFilter.class, lmin=2, lmax=100000)
  public int[] nbins;
  class nbinsFilter extends RSeq { public nbinsFilter() { super("1024", false); } }

  @API(help = "Learning rate, from 0. to 1.0", filter=learn_rateFilter.class, dmin=0, dmax=1)
  public double[] learn_rate;
  class learn_rateFilter extends RSeq { public learn_rateFilter() { super("0.01,0.1,0.2", false); } }
//@formatter:on

  @API(help = "Current model")
  Key currentModel = Key.make(UUID.randomUUID().toString(), (byte) 1, Key.DFJ_INTERNAL_USER);

  public GBMGrid() {
    super(DOC_GET, Key.make("__GBMGrid_" + Key.make()));
    description = DOC_GET;
  }

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='GBMGrid.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", SOURCE_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected void onArgumentsParsed() {
    String source_key = input("source");
    if( source_key != null && destination_key == null ) {
      String n = source_key;
      int dot = n.lastIndexOf('.');
      if( dot > 0 )
        n = n.substring(0, dot);
      String res = n + Extensions.KMEANS_GRID;
      destination_key = Key.make(res);
    }
  }

  @Override protected void run() {
    try {
      ArrayList<String> names = new ArrayList<String>();
      ArrayList<List<String>> data = new ArrayList<List<String>>();
      names.add("ntrees");
      names.add("max_depth");
      names.add("min_rows");
      names.add("nbins");
      names.add("learn_rate");
      names.add("model build time (s)");
      names.add("seconds per tree");
      assert names.size() == KEYS_INDEX;
      names.add("model key");
      names.add("model number");
      names.add("model quality score");
      int count = ntrees.length * max_depth.length * min_rows.length * nbins.length * learn_rate.length;
      int n = 0;
      for( int ntreesI = 0; ntreesI < ntrees.length; ntreesI++ ) {
        for( int max_depthI = 0; max_depthI < max_depth.length; max_depthI++ ) {
          for( int min_rowsI = 0; min_rowsI < min_rows.length; min_rowsI++ ) {
            for( int nbinsI = 0; nbinsI < nbins.length; nbinsI++ ) {
              for( int learn_rateI = 0; learn_rateI < learn_rate.length; learn_rateI++ ) {
                final GBM job = new GBM();
                UKV.put(currentModel, job.destination_key);
                job.source = source;
                job.vresponse = vresponse;
                job.ignored_cols = ignored_cols;
                job.ntrees = ntrees[ntreesI];
                job.max_depth = max_depth[max_depthI];
                job.min_rows = min_rows[min_rowsI];
                job.nbins = nbins[nbinsI];
                job.learn_rate = learn_rate[learn_rateI];
                job.run();
                n++;

                int index = data.size();
                for( ;; ) {
                  if( cancelled() ) {
                    job.cancel();
                    return;
                  }
                  boolean running = job.running();
                  ArrayList<String> values = new ArrayList<String>();
                  values.add("" + job.ntrees);
                  values.add("" + job.max_depth);
                  values.add("" + job.min_rows);
                  values.add("" + job.nbins);
                  values.add("" + job.learn_rate);
                  double model_build_time = (System.currentTimeMillis() - job.start_time) / 1000;
                  values.add("" + model_build_time);
                  double seconds_per_tree = model_build_time / job.ntrees;
                  values.add("" + seconds_per_tree);
                  values.add("" + job.destination_key);
                  values.add("" + n);
                  values.add("" + (running ? -1 : 0));		// TODO ADD SCORE FOR THIS MODEL.
                  if( index == data.size() )
                    data.add(values);
                  else
                    data.set(index, values);
                  if( !running ) {
                    Collections.sort(data, new Comparator<List<String>>() {
                      @Override public int compare(List<String> o1, List<String> o2) {
                        return Double.compare( //
                            Double.parseDouble(o1.get(o1.size() - 1)), //
                            Double.parseDouble(o2.get(o2.size() - 1)));
                      }
                    });
                  }
                  FrameWithProgress frame = new FrameWithProgress(names.toArray(new String[0]), vecs(names, data));
                  frame.currentJob = n;
                  frame.totalJobs = count;
                  UKV.put(destination_key, frame);
                  if( !running )
                    break;
                  try {
                    Thread.sleep(1000);
                  } catch( InterruptedException e ) {
                    throw new RuntimeException(e);
                  }
                }
              }
            }
          }
        }
      }
    } finally {
      UKV.remove(currentModel);
      remove();
    }
  }

  @Override public float progress() {
    FrameWithProgress frame = UKV.get(destination_key);
    return frame == null ? 0 : (float) frame.currentJob / frame.totalJobs;
  }

  @Override protected Response redirect() {
    String n = GBMGridProgress.class.getSimpleName();
    return new Response(Response.Status.redirect, this, -1, -1, n, "job", job_key, "dst_key", destination_key);
  }

  public static class FrameWithProgress extends Frame {
    @API(help = "Current job")
    int currentJob;

    @API(help = "Total jobs")
    int totalJobs;

    public FrameWithProgress(String[] names, Vec[] vecs) {
      super(names, vecs);
    }
  }

  public static class GBMGridProgress extends Progress2 {
    @Override protected String name() {
      return DOC_GET;
    }

    @Override public boolean toHTML(StringBuilder sb) {
      FrameWithProgress frame = UKV.get(Key.make(dst_key.value()));
      if( frame == null )
        DocGen.HTML.section(sb, "Building a first model");
      else
        DocGen.HTML.section(sb, "Building model: " + frame.currentJob + " of " + frame.totalJobs);
      GBMGrid grid = (GBMGrid) Job.findJob(Key.make(job.value()));
      GBMModel model = null;
      if( grid.currentModel != null ) {
        Key dest = UKV.get(grid.currentModel);
        if( dest != null )
          model = UKV.get(dest);
      }
      if( model != null )
        DocGen.HTML.section(sb, "Building tree: " + (model.numTrees() + 1) + " of " + model.N + " for current model");
      if( frame != null ) {
        DocGen.HTML.arrayHead(sb);
        sb.append("<tr class='warning'>");
        for( int i = 0; i < frame.numCols(); i++ )
          sb.append("<td><b>").append(frame._names[i]).append("</b></td>");
        sb.append("</tr>");
        for( int j = 0; j < frame.numRows(); j++ ) {
          sb.append("<tr>");
          for( int i = 0; i < frame.numCols(); i++ ) {
            sb.append("<td>");
            String txt = Inspect2.x0(frame.vecs()[i], j);
            if( i == KEYS_INDEX ) {
              String key = frame.vecs()[i]._domain[(int) frame.vecs()[i].at8(j)];
              sb.append(GBMModelView.link(txt, Key.make(key)));
            } else
              sb.append(txt);
            if( i == KEYS_INDEX )
              sb.append("</a>");
            sb.append("</td>");
          }
          sb.append("</tr>");
        }
        DocGen.HTML.arrayTail(sb);
      }
      return true;
    }

    @Override protected Response jobDone(final Job job, final String dst) {
      return new Response(Response.Status.done, this, 0, 0, null);
    }
  }

  private static Vec[] vecs(ArrayList<String> names, List<List<String>> values) {
    Vec[] vecs = new Vec[names.size()];
    ArrayList<String> enums = new ArrayList<String>();
    for( int v = 0; v < vecs.length; v++ ) {
      vecs[v] = new AppendableVec(UUID.randomUUID().toString());
      NewChunk chunk = new NewChunk(vecs[v], 0);
      for( List<String> value : values ) {
        try {
          double d = Double.parseDouble(value.get(v));
          chunk.addNum(d);
        } catch( NumberFormatException e ) {
          int i = enums.indexOf(value.get(v));
          if( i < 0 ) {
            i = enums.size();
            enums.add(value.get(v));
          }
          chunk.addEnum(i);
        }
      }
      chunk.close(0, null);
      vecs[v] = ((AppendableVec) vecs[v]).close(null);
      if( enums.size() > 0 ) {
        vecs[v]._domain = enums.toArray(new String[0]);
        enums.clear();
      }
    }
    return vecs;
  }
}
