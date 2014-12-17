package water.api;

import water.*;
import water.exec.ASTTable;
import water.exec.ASTddply.Group;
import water.exec.Env;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.nbhm.NonBlockingHashMap;
import water.util.Log;

import java.util.Arrays;

public class Impute extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
  public static final String DOC_GET = "Impute";

  @API(help = "Data Frame containing columns to be imputed.", required = true, filter = Default.class, json=true)
  public Frame source;

  @API(help="Column which to impute.", required=true, filter=columnVecSelect.class, json=true)
  public Vec column;
  class columnVecSelect extends VecClassSelect { columnVecSelect() { super("source"); } }

  @API(help = "Method of impute: Mean, Median, Most Common", required = true, filter = Default.class, json=true) //, Regression, RandomForest
  public Method method = Method.mean;

  class colsFilter1 extends MultiVecSelect { public colsFilter1() { super("source");} }
  @API(help = "Columns to Select for Grouping", filter=colsFilter1.class)
  int[] group_by;


  public enum Method {
    mean,
    median,
    mode
   // regression,
   // randomForest
  }

  public Impute() {}

  protected boolean init() throws IllegalArgumentException {
    // Input handling
    if (source == null || column == null)
      throw new IllegalArgumentException("Missing data or input column!");
    if (column.isBad()) {
      Log.info("Column is 100% NAs, nothing to do.");
      return true;
    }
    if (method != Method.mean && method != Method.median && method != Method.mode)  // || method != Method.regression || method != Method.randomForest
      throw new IllegalArgumentException("method must be one of (mean, median, mode)"); // regression, randomForest)");
    if ( !(column.isEnum()) && column.naCnt() <= 0) {
      Log.info("No NAs in the column, nothing to do.");
      return true;
    }
    if (column.isEnum() && !Arrays.asList(column._domain).contains("NA") && column.naCnt() <= 0 ) {
      Log.info("No NAs in the column, nothing to do.");
      return true;
    }
//    if (method == Method.regression && (column.isEnum() || column.isUUID() || column.isTime()))
//      throw new IllegalArgumentException("Trying to perform regression on non-numeric column! Please select a different column.");
    if (method == Method.mode && (!column.isEnum()))
      throw new IllegalArgumentException("Method `mode` only applicable to factor columns.");
    if (column.isEnum() && method != Method.mode) {
      Log.warn("Column to impute is a factor column, changing method to mode.");
      method = Method.mode;
    }
    return false;
  }

  @Override protected Response serve() {
    if (init()) return Inspect2.redirect(this, source._key.toString());
    final int col_id = source.find(column);
    final int[] _cols = group_by;
    final Key mykey = Key.make();
    try {
      if (group_by == null) {
        // just use "method" using the input "column"
        double _replace_val = 0;

        if (method == Method.mean) {
          _replace_val = column.mean();
        } else if (method == Method.median) {
          QuantilesPage qp = new QuantilesPage();
          qp.source_key = source;
          qp.column = column;
          qp.invoke();
          _replace_val = qp.result;
        } else if (method == Method.mode) {
          String dom[] = column.domain();
          long[][] levels = new long[1][];
          levels[0] = new Vec.CollectDomain(column).doAll(new Frame(column)).domain();
          long[][] counts = new ASTTable.Tabularize(levels).doAll(column)._counts;
          long maxCounts = -1;
          int mode = -1;
          for (int i = 0; i < counts[0].length; ++i) {
            if (counts[0][i] > maxCounts && !dom[i].equals("NA")) { // check for "NA" in domain -- corner case from R
              maxCounts = counts[0][i];
              mode = i;
            }
          }
          _replace_val = mode != -1
                  ? (double) mode
                  : (double) Arrays.asList(dom).indexOf("NA");  // could produce -1 if "NA" not in the domain -- that is we don't have the R corner case
          if (_replace_val == -1) _replace_val = Double.NaN;    // OK to replace, since we're in the elif "mode" block
        }
        final double rv = _replace_val;
        new MRTask2() {
          @Override
          public void map(Chunk[] cs) {
            Chunk c = cs[col_id];
            int rows = c.len();
            for (int r = 0; r < rows; ++r) {
              if (c.isNA0(r) || (c._vec.isEnum() && c._vec.domain()[(int) c.at0(r)].equals("NA"))) {
                if (!Double.isNaN(rv)) c.set0(r, rv); // leave as NA if replace value is NA
              }
            }
          }
        }.doAll(source);
      } else {
        // collect the groups HashMap and the frame from the ddply.
        // create a vec of group IDs (each row is in some group)
        // MRTask over the rows
        water.exec.Exec2.exec(Key.make().toString() + " = anonymous <- function(x) \n{\n " + method + "(x[," + (col_id + 1) + "])\n}").remove_and_unlock();
        Env env = water.exec.Exec2.exec(mykey.toString() + " = ddply(" + source._key.toString() + ", " + toAryString(_cols) + ", anonymous)");
        final Frame grp_replacement = new Frame(env.peekAry());
        env.remove_and_unlock();
        Log.info("GROUP TASK NUM COLS: "+ grp_replacement.numCols());
        final GroupTask grp2val = new GroupTask(grp_replacement.numCols() - 1).doAll(grp_replacement);

        new MRTask2() {
          @Override
          public void map(Chunk[] cs) {
            Chunk c = cs[col_id];
            int rows = cs[0].len();
            for (int r = 0; r < rows; ++r) {
              if (c.isNA0(r) || (c._vec.isEnum() && c._vec.domain()[(int) c.at0(r)].equals("NA"))) {
                Group g = new Group(_cols.length);
                g.fill(r, cs, _cols);
                if (grp2val._grp2val.get(g) == null) continue;
                double rv = grp2val._grp2val.get(g);
                c.set0(r, rv);
              }
            }
          }
        }.doAll(source);
      }
      return Inspect2.redirect(this, source._key.toString());
    } catch( Throwable t ) {
        return Response.error(t);
    } finally {       // Delete frames
      UKV.remove(mykey);
    }
  }

  private String toAryString(int[] c) {
    String res = "c(";
    for (int i = 0; i < c.length; ++i) {
      if (i ==c.length-1) res += String.valueOf(c[i] + 1) + ")";  // + 1 for 0 -> 1 based indexing
      else res += String.valueOf(c[i]+1)+",";                     // + 1 for 0 -> 1 based indexing
    }
    return res;
  }

  @Override public boolean toHTML( StringBuilder sb ) { return super.toHTML(sb); }

  // Create a table: Group -> Impute value
  private static class GroupTask extends MRTask2<GroupTask> {
    protected NonBlockingHashMap<Group, Double> _grp2val = new NonBlockingHashMap<Group, Double>();
    int[] _cols;
    int _ncols;

    GroupTask(int ncols) { _cols = new int[_ncols=ncols]; for (int i = 0; i < _cols.length; ++i) _cols[i] = i;}

    @Override public void map(Chunk[] cs) {
      if (_grp2val == null) _grp2val = new NonBlockingHashMap<Group, Double>();
      if (_cols == null) {
        _cols = new int[cs.length-1];
        for (int i = 0; i < _cols.length; ++i) _cols[i] = i;
      }
      int rows = cs[0].len();
      Chunk vals = cs[cs.length-1];
      for (int row = 0; row < rows; ++row) {
        Group g = new Group(_cols.length);
        g.fill(row, cs, _cols);
        double val = vals.at0(row);
        _grp2val.putIfAbsent(g, val);
      }
    }

    @Override public void reduce( GroupTask gt) {
      for (Group g : gt._grp2val.keySet()) {
        Double val = gt._grp2val.get(g);
        if (g != null && val != null) _grp2val.putIfAbsent(g, val);
      }
    }

    // Custom serialization for NBHM.  Much nicer when these are auto-gen'd.
    // Only sends Groups over the wire, NOT NewChunks with rows.
    @Override public AutoBuffer write( AutoBuffer ab ) {
      super.write(ab);
      if( _grp2val == null ) return ab.put4(0);
      ab.put4(_grp2val.size());
      for( Group g : _grp2val.keySet() ) { ab.put(g); ab.put8d(_grp2val.get(g)); }
      return ab;
    }

    @Override public GroupTask read( AutoBuffer ab ) {
      super.read(ab);
      int len = ab.get4();
      if( len == 0 ) return this;
      _grp2val= new NonBlockingHashMap<Group,Double>();
      for( int i=0; i<len; i++ )
        _grp2val.put(ab.get(Group.class),ab.get8d());
      return this;
    }
    @Override public void copyOver( Freezable dt ) {
      GroupTask that = (GroupTask)dt;
      super.copyOver(that);
      this._ncols = that._ncols;
      this._cols = that._cols;
      this._grp2val = that._grp2val;
    }
  }
}
