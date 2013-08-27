package hex;
import java.util.TreeSet;

import water.*;
import water.api.DocGen;
import water.api.Request.API;
import water.api.RequestArguments.FrameKey;
import water.api.RequestBuilders.Response;
import water.api.Request.VecSelect;
import water.fvec.*;
import water.nbhm.NonBlockingHashSet;
import water.util.RString;

/**
 * Class for computing quantiles (epsilon-approximate, deterministic)
 * @author smcinerney
 *
 * Reference:
 *
 * "A Cost Model for Interval Intersection Queries on RI-Trees",
 * Kriegel, Pfeifle, Pštke, Seidl - SSDBM 2002
 *
 * Mahout OnlineSummarizer (Exponentially Weighted Stochastic Approximation)
 * http://svn.apache.org/repos/asf/mahout/trunk/math/src/main/java/org/apache/mahout/math/stats/OnlineSummarizer.java
 * Welford's method: http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#On-line_algorithm
 * The method used for computing the quartiles is a simplified form of the stochastic approximation
 * method described in the article "Incremental Quantile Estimation for Massive Tracking"
 * by Chen, Lambert and Pinheiro, http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.105.1580
 *
 * An epsilon-approximate deterministic estimator of quantiles on large datasets,
 * with low memory requirements, per:
 * "Approximate medians and other quantiles in one pass and with limited memory",
 * Manku. Rajagopalan & Lindsay, ACM SIGMOD 1998

(A subsequent improvement O(log^2 n / m) on that method is given in:
"Space-Efficient Online Computation of Quantile Summaries"
- Michael Greenwald, Sanjeev Khanna - SIGMOD, 2001)

"Selection and Sorting with Limited Storage",
J. I. Munro and M. S. Paterson, Theoretical Computer Science, vol. 12, pp. 315Ð323, 1980

The 9 methods used in R stats::quantile() are defined in:
"Sample quantiles in statistical packages" - Hyndman & Fan - ASA 1996

A method with much reduced communication overhead but higher error is:
"Medians and Beyond: New Aggregation Techniques for Sensor Networks",
Shrivastava, Buragohain, Agrawal, - 2004 Suri
 */



public class Quantiles extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Quantiles (epsilon-approximate) on arbitrary column";

  @API(help="Data Frame", required=true, filter=FrameKey.class)
  Frame source;

  /* BUG: API will not accept plain VecSelect here:
   * java.lang.Exception: Class water.api.Request$VecSelect must have an empty constructor
   *@API(help="Column", required=true, filter=VecSelect.class)
   *Vec vec; */
  @API(help="Column", required=true, filter=QVecSelect.class)
  Vec vec;
  class QVecSelect extends VecSelect { QVecSelect() { super("source"); } }

  // API can't handle declaring these as double[]
  @API(help="QuantileA", required=true, filter=Default.class)  // BUG: filter=Real.class doesn't appear
  double quantileA = .05;
  @API(help="QuantileB", required=true, filter=Default.class)
  double quantileB = .1;
  @API(help="QuantileC", required=true, filter=Default.class)
  double quantileC = .15;
  @API(help="QuantileD", required=true, filter=Default.class)
  double quantileD = .85;
  @API(help="QuantileE", required=true, filter=Default.class)
  double quantileE = .9;
  @API(help="QuantileF", required=true, filter=Default.class)
  double quantileF = .95;

  //REMOVE @API(help="Buffer size, k")  long k;
  @API(help="Pass 1 msec")     long pass1time;
  @API(help="Pass 2 msec")     long pass2time;
  @API(help="Pass 3 msec")     long pass3time;
  @API(help="nrows (N)")       long nrows;
  //@API(help="epsilon")         double epsilon;
  //@API(help="epsilon*N, ranking error") double epsilon_N;


  @Override public Response serve() {

    //quantiles -> map somehow to bucket sizes

    // Pass 1: ...
    long start = System.currentTimeMillis();
    nrows = vec.length();
    /* Chunk dataset out to fixed-size subtrees */
    MergeSortTask ms1 = new MergeSortTask().doAll(vec);
    long pass1 = System.currentTimeMillis();
    pass1time = pass1 - start;

    // Split the column chunks(?) and buffers out to nodes... make sure the buffers are local

    // ...

    return new Response(Response.Status.done, this, -1, -1, null);
  }

  private static void PrintChunk(String msg, Chunk xs) {
    System.err.print("\n" + msg);
    for (int i=0; i<xs._len; i++) {
      System.err.print((long) xs.at0(i) + ","); // cast to long
    }
    System.err.println();
  }

  public static class MergeSortTask extends MRTask2<MergeSortTask> {
    private double[] _tmp, _tmp2;

    /*private void sort(double[] values) {
      this._tmp = values;
      this._tmp2 = new double[_tmp.length];
      mergesort(0, _tmp.length-1);
    }*/

    private void mergesort(int low, int high) {  // TODO: templatize doubles/longs as <T>
      if (low<high) {
        int middle = low + (high-low)/2;
        mergesort(low, middle);
        mergesort(middle+1, high);
        merge(low,middle,high);
      }
    }
    private void merge(int low, int middle, int high) {
      for (int i=low; i<=high; i++) {
        _tmp2[i] = _tmp[i];
      }

      int i = low;
      int j = middle + 1;
      int k = low;

      for ( ; i<=middle && j<=high; k++) {
        if (_tmp2[i] <= _tmp2[j]) {
          _tmp2[k] = _tmp[i++];
        } else {
          _tmp2[k] = _tmp[j++];
        }
      }

      for ( ; i<=middle; k++, i++) {
        _tmp[k] = _tmp2[i];
      }
    }

    MergeSortTask() {}
    @Override public void map(Chunk xs) {
      System.err.println("Mergesorting chunk 0x" + xs +" of length=" + xs._len); // printing will mess up runtime
      PrintChunk("BEFORE:", xs);

      // Copy in data to local temporary arrays
      int length = xs._len;
      _tmp  = new double[length];
      _tmp2 = new double[length];
      for (int i=0; i<length; i++) {
        _tmp[i] = xs.at0(i); // .at80() for long, at0() for double
      }

      mergesort(0, length-1);

      // Writeback results
      for (int i=0; i<length; i++) {
        xs.set80(i, _tmp2[i]);
      }
      //xs.close(cidx, fs); // ??? int cidx, Futures fs

      PrintChunk("AFTER: ", xs);
      System.err.println();
    }
    @Override public void reduce(MergeSortTask drt) {}
  }

  /*public static class GetQuantilesTask extends MRTask2<GetQuantilesTask> {
  }*/


  /** Return the HTML query link to this page */
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='Quantiles.query?data_key=%$key'>%content</a>");
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

}

