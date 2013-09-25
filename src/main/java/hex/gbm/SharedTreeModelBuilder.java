package hex.gbm;

import water.*;
import water.Job.FrameJob;
import water.api.DocGen;
import water.fvec.*;
import water.util.Log.Tag.Sys;
import water.util.Log;

public abstract class SharedTreeModelBuilder extends FrameJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="", required=true, filter=TVecSelect.class)
  public Vec vresponse;
  class TVecSelect extends VecClassSelect { TVecSelect() { super("source"); } }

  @API(help="columns to ignore",required=false,filter=TMultiVecSelect.class)
  public int [] ignored_cols = new int []{};
  class TMultiVecSelect extends MultiVecSelect { TMultiVecSelect() { super("source");} }

  @API(help = "Number of trees", filter = Default.class, lmin=1, lmax=1000000)
  public int ntrees = 100;

  @API(help = "Maximum tree depth", filter = Default.class, lmin=0, lmax=10000)
  public int max_depth = 5;

  @API(help = "Fewest allowed observations in a leaf", filter = Default.class, lmin=1)
  public int min_rows = 10;

  @API(help = "Build a histogram of this many bins, then split at the best point", filter = Default.class, lmin=2, lmax=100000)
  public int nbins = 1024;

  // Overall prediction error as I add trees
  transient protected float _errs[];

  public SharedTreeModelBuilder( String name, String keyn ) { super(name,Key.make(keyn+Key.make())); }

  public float progress(){
    DTree.TreeModel m = DKV.get(dest()).get();
    return (float)m.treeBits.length/(float)m.N;
  }

  // --------------------------------------------------------------------------
  // Driver for model-building.
  public void startBuildModel( ) {
    final Frame fr = new Frame(source); // Local copy for local hacking
    fr.remove(ignored_cols);
    // Doing classification only right now...
    if( !vresponse.isEnum() ) vresponse.asEnum();

    // While I'd like the Frames built custom for each call, with excluded
    // columns already removed - for now check to see if the response column is
    // part of the frame and remove it up front.
    String vname="response";
    for( int i=0; i<fr.numCols(); i++ )
      if( fr.vecs()[i]==vresponse ) {
        vname=fr._names[i];
        fr.remove(i);
      }

    fr.add(vname,vresponse);         // Hardwire response as last vector
    final Frame frm = new Frame(fr); // Model-Frame; no extra columns

    assert 0 <= ntrees && ntrees < 1000000; // Sanity check
    assert 1 <= min_rows;
    final int  ncols = fr.numCols()-1; // Feature set
    final long nrows = fr.numRows();
    final int ymin = (int)vresponse.min();
    final char nclass = vresponse.isInt() ? (char)(vresponse.max()-ymin+1) : 1;
    assert 1 <= nclass && nclass <= 1000; // Arbitrary cutoff for too many classes
    _errs = new float[0];     // No trees yet
    final Key outputKey = dest();
    final Key dataKey = null;

    // Tail-call position: this forks off in the background, and this call
    // returns immediately.  The actual model build is merely kicked off.
    buildModel(fr,frm,outputKey, dataKey, ncols, nrows, nclass, ymin, new Timer());
  }

  // Shared cleanup
  protected void cleanUp(Frame fr, int ncols, Timer t_build) {
    Log.info(logTag(),"Modeling done in "+t_build);

    // Remove temp vectors; cleanup the Frame
    while( fr.numCols() > ncols+1/*Do not delete the response vector*/ )
      UKV.remove(fr.remove(fr.numCols()-1)._key);
    remove();                   // Remove Job
  }

  protected abstract water.util.Log.Tag.Sys logTag();
  protected abstract void buildModel( final Frame fr, final Frame frm, final Key outputKey, final Key dataKey, final int ncols, final long nrows, final char nclass, final int ymin, final Timer t_build );
}
