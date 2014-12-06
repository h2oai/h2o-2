package hex;

import water.*;
import water.Job.ColumnsJob;
import water.api.DocGen;
import water.api.Progress2;
import water.api.Request;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.Vec;
import water.util.Log;
import water.util.RString;
import water.util.Utils;

import java.util.ArrayList;
import java.util.Random;
import java.util.Arrays;


/**
 * Scalable K-Means++ (KMeans||)<br>
 * http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf<br>
 * http://www.youtube.com/watch?v=cigXAxV3XcY
 */
public class KMeans2 extends ColumnsJob {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;
  static final String DOC_GET = "k-means";
  public enum Initialization {
    None, PlusPlus, Furthest
  };

  @API(help = "Cluster initialization: None - chooses initial centers at random; Plus Plus - choose first center at random, subsequent centers chosen from probability distribution weighted so that points further from first center are more likey to be selected; Furthest - chooses initial point at random, subsequent point taken as the point furthest from prior point.", filter = Default.class, json=true)
  // public Initialization initialization = Initialization.None;
  public Initialization initialization = Initialization.Furthest;
  // default Initialization is Furthest. Better results for hard cases, especially with just one trial
  // in the browser. PlusPlus can be biased, so Furthest can be best, again especially if just one trial
  // Random should never be better than Furthest.

  @API(help = "Number of clusters", required = true, filter = Default.class, lmin = 1, lmax = 100000, json=true)
  public int k = 2;

  @API(help = "Maximum number of iterations before stopping", required = true, filter = Default.class, lmin = 1, lmax = 100000, json=true)
  public int max_iter = 100;

  @API(help = "Whether data should be normalized", filter = Default.class, json=true)
  public boolean normalize;

  @API(help = "Seed for the random number generator", filter = Default.class, json=true)
  public long seed = new Random().nextLong();

  @API(help = "Drop columns with more than 20% missing values", filter = Default.class, json=true)
  public boolean drop_na_cols = true;

  // Number of categorical columns
  private int _ncats;

  // Number of reinitialization attempts for preventing empty clusters
  transient private int reinit_attempts;

  // Make a link that lands on this page
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='KMeans2.query?source=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  public KMeans2() {
    description = "K-means";
  }

  // ----------------------
  @Override public void execImpl() {
    Frame fr;
    KMeans2Model model = null;
    try {

      logStart();
      source.read_lock(self());
      if ( source.numRows() < k) throw new IllegalArgumentException("Cannot make " + k + " clusters out of " + source.numRows() + " rows.");

      // Drop ignored cols and, if user asks for it, cols with too many NAs
      fr = FrameTask.DataInfo.prepareFrame(source, ignored_cols, false, drop_na_cols);
//      fr = source;
      if (fr.numCols() == 0) throw new IllegalArgumentException("No columns left to work with.");

      // Sort columns, so the categoricals are all up front.  They use a
      // different distance metric than numeric columns.
      Vec vecs[] = fr.vecs();
      final int N = vecs.length; // Feature count
      int ncats=0, len=N;
      while( ncats != len ) {
        while( ncats < len && vecs[ncats].isEnum() ) ncats++;
        while( len > 0 && !vecs[len-1].isEnum() ) len--;
        if( ncats < len-1 ) fr.swap(ncats,len-1);
      }
      _ncats = ncats;

      // The model to be built
      model = new KMeans2Model(this, dest(), fr._key, fr.names(), fr.domains());

      model.delete_and_lock(self());

      // means are used to impute NAs
      double[] means = new double[N];
      for( int i = 0; i < N; i++ )
        means[i] = vecs[i].mean();
      // mults & means for normalization
      double[] mults = null;
      if( normalize ) {
        mults = new double[N];
        for( int i = 0; i < N; i++ ) {
          double sigma = vecs[i].sigma();
          mults[i] = normalize(sigma) ? 1.0 / sigma : 1.0;
        }
      }

      // Initialize clusters
      Random rand = Utils.getRNG(seed - 1);
      double clusters[][];    // Normalized cluster centers
      if( initialization == Initialization.None ) {
        // Initialize all clusters to random rows. Get 3x the number needed
        clusters = model.centers = new double[k*3][fr.numCols()];
        for( double[] cluster : clusters )
          randomRow(vecs, rand, cluster, means, mults);
        // for( int i=0; i<model.centers.length; i++ ) {
        //   Log.info("random model.centers["+i+"]: "+Arrays.toString(model.centers[i]));
        // }
        // Recluster down to K normalized clusters. 
        clusters = recluster(clusters, rand);
      } else {
        clusters = new double[1][vecs.length];
        // Initialize first cluster to random row
        randomRow(vecs, rand, clusters[0], means, mults);

        while( model.iterations < 5 ) {
          // Sum squares distances to clusters
          SumSqr sqr = new SumSqr(clusters,means,mults,_ncats).doAll(vecs);
          // Log.info("iteration: "+model.iterations+" sqr: "+sqr._sqr);

          // Sample with probability inverse to square distance
          long randomSeed = (long) rand.nextDouble();
          Sampler sampler = new Sampler(clusters, means, mults, _ncats, sqr._sqr, k * 3, randomSeed).doAll(vecs);
          clusters = Utils.append(clusters,sampler._sampled);

          // Fill in sample clusters into the model
          if( !isRunning() ) return; // Stopped/cancelled
          model.centers = denormalize(clusters, ncats, means, mults);
          // see below. this is sum of squared error now
          model.total_within_SS = sqr._sqr;
          model.iterations++;     // One iteration done

          // Log.info("\nKMeans Centers during init models.iterations: "+model.iterations);
          // for( int i=0; i<model.centers.length; i++ ) {
          //   Log.info("model.centers["+i+"]: "+Arrays.toString(model.centers[i]));
          // }
          // Log.info("model.total_within_SS: "+model.total_within_SS);

          // Don't count these iterations as work for model building
          model.update(self()); // Early version of model is visible

          // Recluster down to K normalized clusters. 
          // makes more sense to recluster each iteration, since the weighted k*3 effect on sqr vs _sqr
          // reflects the k effect on _sqr? ..if there are too many "centers" (samples) then _sqr (sum of all) is too 
          // big relative to sqr (possible new point, and we don't gather any more samples? 
          // (so the centers won't change during the init)
          clusters = recluster(clusters, rand);
        }
      }
      model.iterations = 0;     // Reset iteration count

      // ---
      // Run the main KMeans Clustering loop
      // Stop after enough iterations
      boolean done;
      LOOP:
      for( ; model.iterations < max_iter; model.iterations++ ) {
        if( !isRunning() ) return; // Stopped/cancelled
        Lloyds task = new Lloyds(clusters,means,mults,_ncats, k).doAll(vecs);
        // Pick the max categorical level for clusters' center
        max_cats(task._cMeans,task._cats);

        // Handle the case where some clusters go dry.  Rescue only 1 cluster
        // per iteration ('cause we only tracked the 1 worst row)
        boolean badrow=false;
        for( int clu=0; clu<k; clu++ ) {
          if (task._rows[clu] == 0) {
            // If we see 2 or more bad rows, just re-run Lloyds to get the
            // next-worst row.  We don't count this as an iteration, because
            // we're not really adjusting the centers, we're trying to get
            // some centers *at-all*.
            if (badrow) {
              Log.warn("KMeans: Re-running Lloyds to re-init another cluster");
              model.iterations--; // Do not count against iterations
              if (reinit_attempts++ < k) {
                continue LOOP;  // Rerun Lloyds, and assign points to centroids
              } else {
                reinit_attempts = 0;
                break; //give up and accept empty cluster
              }
            }
            long row = task._worst_row;
            Log.warn("KMeans: Re-initializing cluster " + clu + " to row " + row);
            data(clusters[clu] = task._cMeans[clu], vecs, row, means, mults);
            task._rows[clu] = 1;
            badrow = true;
          }
        }

        // Fill in the model; denormalized centers
        model.centers = denormalize(task._cMeans, ncats, means, mults);
        model.size = task._rows;
        model.within_cluster_variances = task._cSqr;
        double ssq = 0;       // sum squared error
        for( int i=0; i<k; i++ ) {
          ssq += model.within_cluster_variances[i]; // sum squared error all clusters
//          model.within_cluster_variances[i] /= task._rows[i]; // mse per-cluster
        }
//        model.total_within_SS = ssq/fr.numRows(); // mse total
        model.total_within_SS = ssq; //total within sum of squares

        model.update(self()); // Update model in K/V store
        reinit_attempts = 0;

        // Compute change in clusters centers
        double sum=0;
        for( int clu=0; clu<k; clu++ )
          sum += distance(clusters[clu],task._cMeans[clu],ncats);
        sum /= N;             // Average change per feature
        Log.info("KMeans: Change in cluster centers="+sum);
        done = ( sum < 1e-6 || model.iterations == max_iter-1);

        if (done) {
          Log.info("Writing clusters to key " + model._clustersKey);
          Clusters cc = new Clusters();
          cc._clusters = clusters;
          cc._means = means;
          cc._mults = mults;
          cc.doAll(1, vecs);
          Frame fr2 = cc.outputFrame(model._clustersKey, new String[]{"Cluster ID"}, new String[][]{Utils.toStringMap(0, cc._clusters.length - 1)});
          fr2.delete_and_lock(self()).unlock(self());
          break;
        }

        clusters = task._cMeans; // Update cluster centers

        StringBuilder sb = new StringBuilder();
        sb.append("KMeans: iter: ").append(model.iterations).append(", MSE=").append(model.total_within_SS);
        for( int i=0; i<k; i++ )
          sb.append(", ").append(task._cSqr[i]).append("/").append(task._rows[i]);
        Log.info(sb);
      }

    } catch( Throwable t ) {
      t.printStackTrace();
      cancel(t);
    } finally {
      remove();                   // Remove Job
      if( model != null ) model.unlock(self());
      source.unlock(self());
      state = UKV.<Job>get(self()).state;
      new TAtomic<KMeans2Model>() {
        @Override
        public KMeans2Model atomic(KMeans2Model m) {
          if (m != null) m.get_params().state = state;
          return m;
        }
      }.invoke(dest());
    }
  }

  @Override protected Response redirect() {
    return KMeans2Progress.redirect(this, job_key, destination_key);
  }

  public static class KMeans2Progress extends Progress2 {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @Override protected Response jobDone(Key dst) {
      return KMeans2ModelView.redirect(this, destination_key);
    }

    public static Response redirect(Request req, Key job_key, Key destination_key) {
      return Response.redirect(req, new KMeans2Progress().href(), JOB_KEY, job_key, DEST_KEY, destination_key);
    }
  }

  public static class KMeans2ModelView extends Request2 {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "KMeans2 Model", json = true, filter = Default.class)
    public KMeans2Model model;

    @API(help="KMeans2 Model Key", required = true, filter = KMeans2Filter.class)
    Key _modelKey;
    class KMeans2Filter extends H2OKey { public KMeans2Filter() { super("",true); } }

    public static String link(String txt, Key model) {
      return "<a href='" + new KMeans2ModelView().href() + ".html?_modelKey=" + model + "'>" + txt + "</a>";
    }

    public static Response redirect(Request req, Key model) {
      return Response.redirect(req, "/2/KMeans2ModelView", "_modelKey", model);
//      return Response.redirect(req, new KMeans2ModelView().href(), "_modelKey", model);
    }

    @Override protected Response serve() {
      model = DKV.get(_modelKey).get();
      return Response.done(this);
    }

    @Override public boolean toHTML(StringBuilder sb) {
      if( model != null && model.centers != null && model.within_cluster_variances != null) {
        model.parameters.makeJsonBox(sb);
        DocGen.HTML.section(sb, "Cluster Centers: "); //"Total Within Cluster Sum of Squares: " + model.total_within_SS);
        table(sb, "Clusters", model._names, model.centers);
        double[][] rows = new double[model.within_cluster_variances.length][1];
        for( int i = 0; i < rows.length; i++ )
          rows[i][0] = model.within_cluster_variances[i];
        columnHTMLlong(sb, "Cluster Size", model.size);
        DocGen.HTML.section(sb, "Cluster Variances: ");
        table(sb, "Clusters", new String[]{"Within Cluster Sum of Squares"}, rows);
//        columnHTML(sb, "Between Cluster Variances", model.between_cluster_variances);
        sb.append("<br />");
        DocGen.HTML.section(sb, "Overall Totals: ");
        double[] row = new double[]{model.total_within_SS};
        rowHTML(sb, new String[]{"Total Within Cluster Sum of Squares"}, row);
//        double[] row = new double[]{model.total_SS, model.total_within_SS, model.between_cluster_SS};
//        rowHTML(sb, new String[]{"Total Sum of Squares", "Total Within Cluster Sum of Squares", "Between Cluster Sum of Squares"}, row);
        DocGen.HTML.section(sb, "Cluster Assignments by Observation: ");
        RString rs = new RString("<a href='Inspect2.html?src_key=%$key'>%content</a>");
        rs.replace("key", model._key + "_clusters");
        rs.replace("content", "View the row-by-row cluster assignments");
        sb.append(rs.toString());
        //sb.append("<iframe src=\"" + "/Inspect.html?key=KMeansClusters\"" + "width = \"850\" height = \"550\" marginwidth=\"25\" marginheight=\"25\" scrolling=\"yes\"></iframe>" );
        return true;
      }
      return false;
    }

    private static void rowHTML(StringBuilder sb, String[] header, double[] ro) {
      sb.append("<span style='display: inline-block; '>");
      sb.append("<table class='table table-striped table-bordered'>");
      sb.append("<tr>");
      for (String aHeader : header) sb.append("<th>").append(aHeader).append("</th>");
      sb.append("</tr>");
      sb.append("<tr>");
      for (double row : ro) {
        sb.append("<td>").append(ElementBuilder.format(row)).append("</td>");
      }
      sb.append("</tr>");
      sb.append("</table></span>");
    }

    private static void columnHTML(StringBuilder sb, String name, double[] rows) {
      sb.append("<span style='display: inline-block; '>");
      sb.append("<table class='table table-striped table-bordered'>");
      sb.append("<tr>");
      sb.append("<th>").append(name).append("</th>");
      sb.append("</tr>");
      sb.append("<tr>");
      for (double row : rows) {
        sb.append("<tr>");
        sb.append("<td>").append(ElementBuilder.format(row)).append("</td>");
        sb.append("</tr>");
      }
      sb.append("</table></span>");
    }

    private static void columnHTMLlong(StringBuilder sb, String name, long[] rows) {
      sb.append("<span style='display: inline-block; '>");
      sb.append("<table class='table table-striped table-bordered'>");
      sb.append("<tr>");
      sb.append("<th>").append(name).append("</th>");
      sb.append("</tr>");
      sb.append("<tr>");
      for (double row : rows) {
        sb.append("<tr>");
        sb.append("<td>").append(ElementBuilder.format(row)).append("</td>");
        sb.append("</tr>");
      }
      sb.append("</table></span>");
    }

    private static void table(StringBuilder sb, String title, String[] names, double[][] rows) {
      sb.append("<span style='display: inline-block;'>");
      sb.append("<table class='table table-striped table-bordered'>");
      sb.append("<tr>");
      sb.append("<th>").append(title).append("</th>");
      for( int i = 0; names != null && i < rows[0].length; i++ )
        sb.append("<th>").append(names[i]).append("</th>");
      sb.append("</tr>");
      for( int r = 0; r < rows.length; r++ ) {
        sb.append("<tr>");
        sb.append("<td>").append(r).append("</td>");
        for( int c = 0; c < rows[r].length; c++ )
          sb.append("<td>").append(ElementBuilder.format(rows[r][c])).append("</td>");
        sb.append("</tr>");
      }
      sb.append("</table></span>");
    }
  }

  public static class KMeans2Model extends Model implements Progress {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Model parameters")
    private final KMeans2 parameters;    // This is used purely for printing values out.

    @API(help = "Cluster centers, always denormalized")
    public double[][] centers;

    @API(help = "Sum of within cluster sum of squares")
    public double total_within_SS;

//    @API(help = "Between cluster sum of square distances")
//    public double between_cluster_SS;

//    @API(help = "Total Sum of squares = total_within_SS + betwen_cluster_SS")
//    public double total_SS;

    @API(help = "Number of clusters")
    public int k;

    @API(help = "Numbers of observations in each cluster.")
    public long[] size;

    @API(help = "Whether data was normalized")
    public boolean normalized;

    @API(help = "Maximum number of iterations before stopping")
    public int max_iter = 100;

    @API(help = "Iterations the algorithm ran")
    public int iterations;

    @API(help = "Within cluster sum of squares per cluster")
    public double[] within_cluster_variances; //Warning: See note below
    //Note: The R wrapper interprets this as withinss (sum of squares), so that's what we compute here, and NOT the variances.
    //FIXME: => wrong name, should be within_cluster_sum_of_squares, but leaving to be backward-compatible with REST API


//    @API(help = "Between Cluster square distances per cluster")
//    public double[] between_cluster_variances;

    @API(help = "The row-by-row cluster assignments")
    public final Key _clustersKey;

    private transient int _ncats;

    public KMeans2Model(KMeans2 params, Key selfKey, Key dataKey, String names[], String domains[][]) {
      super(selfKey, dataKey, names, domains, /* priorClassDistribution */ null, /* modelClassDistribution */ null);
      _ncats = params._ncats;
      parameters = params;

      // only for backward-compatibility of JSON response
      k = params.k;
      normalized = params.normalize;
      max_iter = params.max_iter;

      _clustersKey = Key.make(selfKey.toString() + "_clusters");
    }

    @Override public final KMeans2 get_params() { return parameters; }
    @Override public final Request2 job() { return get_params(); }

    @Override public double mse() { return total_within_SS; }

    @Override public float progress() {
      return Math.min(1f, iterations / (float) parameters.max_iter);
    }

    @Override protected float[] score0(Chunk[] chunks, int rowInChunk, double[] tmp, float[] preds) {
      assert chunks.length>=_names.length;
      for( int i=0; i<_names.length; i++ )
        tmp[i] = chunks[i].at0(rowInChunk);
      return score0(tmp,preds);
    }

    @Override protected float[] score0(double[] data, float[] preds) {
      preds[0] = closest(centers,data,_ncats);
      return preds;
    }

    @Override public int nfeatures() { return _names.length; }
    @Override public boolean isSupervised() { return false; }
    @Override public String responseName() { throw new IllegalArgumentException("KMeans doesn't have a response."); }

    /** Remove any Model internal Keys */
    @Override public Futures delete_impl(Futures fs) {
      Lockable.delete(_clustersKey);
      return fs;
    }
  }

  public class Clusters extends MRTask2<Clusters> {
    // IN
    double[][] _clusters;         // Cluster centers
    double[] _means, _mults;      // Normalization
    int _ncats, _nnums;

    @Override public void map(Chunk[] cs, NewChunk ncs) {
      double[] values = new double[_clusters[0].length];
      ClusterDist cd = new ClusterDist();
      for (int row = 0; row < cs[0]._len; row++) {
        data(values, cs, row, _means, _mults);
        // closest(_clusters, values, cd);
        closest(_clusters, values, _ncats, cd);
        int clu = cd._cluster;
        // ncs[0].addNum(clu);
        ncs.addEnum(clu);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Initial sum-of-square-distance to nearest cluster
  private static class SumSqr extends MRTask2<SumSqr> {
    // IN
    double[][] _clusters;
    double[] _means, _mults; // Normalization
    final int _ncats;

    // OUT
    double _sqr;

    SumSqr( double[][] clusters, double[] means, double[] mults, int ncats ) {
      _clusters = clusters;
      _means = means;
      _mults = mults;
      _ncats = ncats;
    }

    @Override public void map(Chunk[] cs) {
      double[] values = new double[cs.length];
      ClusterDist cd = new ClusterDist();
      for( int row = 0; row < cs[0].len(); row++ ) {
        data(values, cs, row, _means, _mults);
        _sqr += minSqr(_clusters, values, _ncats, cd);
      }
      _means = _mults = null;
      _clusters = null;
    }

    @Override public void reduce(SumSqr other) { _sqr += other._sqr; }
  }

  // -------------------------------------------------------------------------
  // Sample rows with increasing probability the farther they are from any
  // cluster.
  private static class Sampler extends MRTask2<Sampler> {
    // IN
    double[][] _clusters;
    double[] _means, _mults; // Normalization
    final int _ncats;
    final double _sqr;           // Min-square-error
    final double _probability;   // Odds to select this point
    final long _seed;

    // OUT
    double[][] _sampled;   // New clusters

    Sampler( double[][] clusters, double[] means, double[] mults, int ncats, double sqr, double prob, long seed ) {
      _clusters = clusters;
      _means = means;
      _mults = mults;
      _ncats = ncats;
      _sqr = sqr;
      _probability = prob;
      _seed = seed;
    }

    @Override public void map(Chunk[] cs) {
      double[] values = new double[cs.length];
      ArrayList<double[]> list = new ArrayList<double[]>();
      Random rand = Utils.getRNG(_seed + cs[0]._start);
      ClusterDist cd = new ClusterDist();

      for( int row = 0; row < cs[0].len(); row++ ) {
        data(values, cs, row, _means, _mults);
        double sqr = minSqr(_clusters, values, _ncats, cd);
        if( _probability * sqr > rand.nextDouble() * _sqr ) {
          list.add(values.clone());
          // Log.info("Sampler map adding to the list used for an init iteration values: "+
          //   Arrays.toString(values)+" _probability: "+_probability+" sqr: "+sqr+" _sqr: "+_sqr);
        }
      }
      // Log.info("Sampler map summary: that's another "+list.size()+" to the list used for an init iteration values");

      _sampled = new double[list.size()][];
      list.toArray(_sampled);
      _clusters = null;
      _means = _mults = null;
    }

    @Override public void reduce(Sampler other){
      _sampled = Utils.append(_sampled, other._sampled);
    }
  }

  public static class Lloyds extends MRTask2<Lloyds> {
    // IN
    double[][] _clusters;
    double[] _means, _mults;      // Normalization
    final int _ncats, _K;

    // OUT
    double[][] _cMeans;         // Means for each cluster
    long[/*K*/][/*ncats*/][] _cats; // Histogram of cat levels
    double[] _cSqr;             // Sum of squares for each cluster
    long[] _rows;               // Rows per cluster
    long _worst_row;            // Row with max err
    double _worst_err;          // Max-err-row's max-err

    Lloyds( double[][] clusters, double[] means, double[] mults, int ncats, int K ) {
      _clusters = clusters;
      _means = means;
      _mults = mults;
      _ncats = ncats;
      _K = K;
    }

    @Override public void map(Chunk[] cs) {
      int N = cs.length;
      assert _clusters[0].length==N;
      _cMeans = new double[_K][N];
      _cSqr = new double[_K];
      _rows = new long[_K];
      // Space for cat histograms
      _cats = new long[_K][_ncats][];
      for( int clu=0; clu<_K; clu++ )
        for( int col=0; col<_ncats; col++ )
          _cats[clu][col] = new long[cs[col]._vec.cardinality()];
      _worst_err = 0;

      // Find closest cluster for each row
      double[] values = new double[N];
      ClusterDist cd = new ClusterDist();
      for( int row = 0; row < cs[0].len(); row++ ) {
        data(values, cs, row, _means, _mults);
        closest(_clusters, values, _ncats, cd);
        int clu = cd._cluster;
        assert clu != -1; // No broken rows
        _cSqr[clu] += cd._dist;

        // Add values and increment counter for chosen cluster
        for( int col = 0; col < _ncats; col++ )
          _cats[clu][col][(int)values[col]]++; // Histogram the cats
        for( int col = _ncats; col < N; col++ )
          _cMeans[clu][col] += values[col];
        _rows[clu]++;
        // Track worst row
        if( cd._dist > _worst_err) { _worst_err = cd._dist; _worst_row = cs[0]._start+row; }
      }
      // Scale back down to local mean
      for( int clu = 0; clu < _K; clu++ )
        if( _rows[clu] != 0 ) Utils.div(_cMeans[clu],_rows[clu]);
      _clusters = null;
      _means = _mults = null;
    }

    @Override public void reduce(Lloyds mr) {
      for( int clu = 0; clu < _K; clu++ ) {
        long ra =    _rows[clu];
        long rb = mr._rows[clu];
        double[] ma =    _cMeans[clu];
        double[] mb = mr._cMeans[clu];
        for( int c = 0; c < ma.length; c++ ) // Recursive mean
          if( ra+rb > 0 ) ma[c] = (ma[c] * ra + mb[c] * rb) / (ra + rb);
      }
      Utils.add(_cats, mr._cats);
      Utils.add(_cSqr, mr._cSqr);
      Utils.add(_rows, mr._rows);
      // track global worst-row
      if( _worst_err < mr._worst_err) { _worst_err = mr._worst_err; _worst_row = mr._worst_row; }
    }
  }

  // A pair result: nearest cluster, and the square distance
  private static final class ClusterDist { int _cluster; double _dist;  }

  private static double minSqr(double[][] clusters, double[] point, int ncats, ClusterDist cd) {
    return closest(clusters, point, ncats, cd, clusters.length)._dist;
  }

  private static double minSqr(double[][] clusters, double[] point, int ncats, ClusterDist cd, int count) {
    return closest(clusters,point,ncats,cd,count)._dist;
  }

  private static ClusterDist closest(double[][] clusters, double[] point, int ncats, ClusterDist cd) {
    return closest(clusters, point, ncats, cd, clusters.length);
  }

  private static double distance(double[] cluster, double[] point, int ncats) {
    double sqr = 0;             // Sum of dimensional distances
    int pts = point.length;     // Count of valid points

    // Categorical columns first.  Only equals/unequals matters (i.e., distance is either 0 or 1).
    for(int column = 0; column < ncats; column++) {
      double d = point[column];
      if( Double.isNaN(d) ) pts--;
      else if( d != cluster[column] )
        sqr += 1.0;           // Manhattan distance
    }
    // Numeric column distance
    for( int column = ncats; column < cluster.length; column++ ) {
      double d = point[column];
      if( Double.isNaN(d) ) pts--; // Do not count
      else {
        double delta = d - cluster[column];
        sqr += delta * delta;
      }
    }
    // Scale distance by ratio of valid dimensions to all dimensions - since
    // we did not add any error term for the missing point, the sum of errors
    // is small - ratio up "as if" the missing error term is equal to the
    // average of other error terms.  Same math another way:
    //   double avg_dist = sqr / pts; // average distance per feature/column/dimension
    //   sqr = sqr * point.length;    // Total dist is average*#dimensions
    if( 0 < pts && pts < point.length )
      sqr *= point.length / pts;
    return sqr;
  }

  /** Return both nearest of N cluster/centroids, and the square-distance. */
  private static ClusterDist closest(double[][] clusters, double[] point, int ncats, ClusterDist cd, int count) {
    int min = -1;
    double minSqr = Double.MAX_VALUE;
    for( int cluster = 0; cluster < count; cluster++ ) {
      double sqr = distance(clusters[cluster],point,ncats);
      if( sqr < minSqr ) {      // Record nearest cluster
        min = cluster;
        minSqr = sqr;
      }
    }
    cd._cluster = min;          // Record nearest cluster
    cd._dist = minSqr;          // Record square-distance
    return cd;                  // Return for flow-coding
  }

  // For KMeansModel scoring; just the closest cluster
  static int closest(double[][] clusters, double[] point, int ncats) {
    int min = -1;
    double minSqr = Double.MAX_VALUE;
    for( int cluster = 0; cluster < clusters.length; cluster++ ) {
      double sqr = distance(clusters[cluster],point,ncats);
      if( sqr < minSqr ) {      // Record nearest cluster
        min = cluster;
        minSqr = sqr;
      }
    }
    return min;
  }

  // KMeans++ re-clustering
  private double[][] recluster(double[][] points, Random rand) {
    double[][] res = new double[k][];
    res[0] = points[0];
    int count = 1;
    ClusterDist cd = new ClusterDist();
    switch( initialization ) {
      case PlusPlus: { // k-means++
        while( count < res.length ) {
          double sum = 0;
          for (double[] point1 : points) sum += minSqr(res, point1, _ncats, cd, count);

          for (double[] point : points) {
            if (minSqr(res, point, _ncats, cd, count) >= rand.nextDouble() * sum) {
              res[count++] = point;
              break;
            }
          }
        }
        break;
      }
      // if we oversampled for initialization=None, recluster using the Furthest criteria down to k
      case None:
      case Furthest: { // Takes cluster further from any already chosen ones
        while( count < res.length ) {
          double max = 0;
          int index = 0;
          for( int i = 0; i < points.length; i++ ) {
            double sqr = minSqr(res, points[i], _ncats, cd, count);
            if( sqr > max ) {
              max = sqr;
              index = i;
            }
          }
          res[count++] = points[index];
        }
        break;
      }
      default:  throw H2O.fail();
    }
    return res;
  }

  private void randomRow(Vec[] vecs, Random rand, double[] cluster, double[] means, double[] mults) {
    long row = Math.max(0, (long) (rand.nextDouble() * vecs[0].length()) - 1);
    data(cluster, vecs, row, means, mults);
  }

  private static boolean normalize(double sigma) {
    // TODO unify handling of constant columns
    return sigma > 1e-6;
  }

  // Pick most common cat level for each cluster_centers' cat columns
  private static double[][] max_cats(double[][] clusters, long[][][] cats) {
    int K = cats.length;
    int ncats = cats[0].length;
    for( int clu = 0; clu < K; clu++ )
      for( int col = 0; col < ncats; col++ ) // Cats use max level for cluster center
        clusters[clu][col] = Utils.maxIndex(cats[clu][col]);
    return clusters;
  }

  private static double[][] denormalize(double[][] clusters, int ncats, double[] means, double[] mults) {
    int K = clusters.length;
    int N = clusters[0].length;
    double[][] value = new double[K][N];
    for( int clu = 0; clu < K; clu++ ) {
      System.arraycopy(clusters[clu],0,value[clu],0,N);
      if( mults!=null )         // Reverse normalization
        for( int col = ncats; col < N; col++ )
          value[clu][col] = value[clu][col] / mults[col] + means[col];
    }
    return value;
  }

  private static void data(double[] values, Vec[] vecs, long row, double[] means, double[] mults) {
    for( int i = 0; i < values.length; i++ ) {
      double d = vecs[i].at(row);
      values[i] = data(d, i, means, mults, vecs[i].cardinality());
    }
  }

  private static void data(double[] values, Chunk[] chks, int row, double[] means, double[] mults) {
    for( int i = 0; i < values.length; i++ ) {
      double d = chks[i].at0(row);
      values[i] = data(d, i, means, mults, chks[i]._vec.cardinality());
    }
  }

  /**
   * Takes mean if NaN, normalize if requested.
   */
  private static double data(double d, int i, double[] means, double[] mults, int cardinality) {
    if(cardinality == -1) {
      if( Double.isNaN(d) )
        d = means[i];
      if( mults != null ) {
        d -= means[i];
        d *= mults[i];
      }
    } else {
      // TODO: If NaN, then replace with majority class?
      if(Double.isNaN(d))
        d = Math.min(Math.round(means[i]), cardinality-1);
    }
    return d;
  }
}
