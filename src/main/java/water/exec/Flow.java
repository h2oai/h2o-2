package water.exec;

import water.*;
import water.fvec.*;
import water.util.Utils.*;

public abstract class Flow extends Iced {

  // Flow-Coding with Frames, Filters & GroupBy
  //
  // frame.with(filter).with(groupby).with(reducer).doit();
  // 
  // Define a pipeline of operations to perform on a frame.  Can use any number
  // of filters, and one optional groupby, and one required reducer which must
  // be last in the pipe.  The return result is either the instance of the
  // reducer, or a collection of reducers (one per Group in the GroupBy).
  //
  // All elements are passed a row from the Frame in double ds[].
  //
  // Reducer uses "mapreduce(ds)" to collect data for one row into a reducer
  // object, and "this.reduce(that)" to gather chunks of reduced objects.  Rows
  // are reduced in parallel and in any order; each row exactly once.
  //
  // Filter returns boolean to ignore or reduce the row.
  // 
  // GroupBy returns a long to specify which Group this row belongs too.  The
  // row is reduced into a seperate reducer for each group.  The group ids do
  // not have to form a dense space, any unique long value specifies a group; a
  // hash table is used to gather the groups.  The hashtable of reduced values
  // is returned.
  //
  //Frame fr = parseFrame("cars.hex", "smalldata/cars.csv");
  //final int cyl_idx = fr.find("cylinders");
  //final int year_idx = fr.find("year");
  //  
  //SumCol sumcols = fr.
  //  with(new SumCol(year_idx)).
  //  doit();
  //System.out.println(sumcols._sum+"/"+sumcols._n+" = "+(sumcols._sum/sumcols._n));
  //
  //SumCol sumcols1 = fr.
  //  with(new Filter() { boolean filter(double ds[]) { return ds[cyl_idx]!=5; } }).
  //  with(new SumCol(year_idx)).
  //  doit();
  //System.out.println(sumcols1._sum+"/"+sumcols1._n+" = "+(sumcols1._sum/sumcols1._n));
  //    
  //IcedHashMap<IcedLong,SumCol> sumcols2 = fr.
  //  with(new GroupBy() { long groupId(double ds[]) { return (long)ds[cyl_idx];} }).
  //  with(new SumCol(year_idx)).
  //  doit();
  //for( IcedLong gid : sumcols2.keySet() ) {
  //  SumCol sumcol = sumcols2.get(gid);
  //  System.out.println("Cyl="+gid._val+", "+sumcol._sum+"/"+sumcol._n+" = "+(sumcol._sum/sumcol._n));
  //}
      
  // -----------------------
  // THE PUBLIC API: 

  public abstract static class PerRow<X extends PerRow> extends Iced {
    abstract public void mapreduce( double ds[] );
    abstract public void reduce( X that );
    abstract public X make();
    @Override public String toString() { return "perRow"; }
  }

  public abstract static class Filter extends Iced {
    abstract public boolean filter( double ds[] );
    @Override public String toString() { return "filter"; }
  }

  public abstract static class GroupBy extends Iced {
    abstract public long groupId( double ds[]);
    @Override public String toString() { return "groupBy"; }
  }
  // -----------------------


  abstract Frame frame();

  abstract
  <X extends PerRow<X>>                           // Type parameter
  PerRow<X>                                       // Return type of doit()
  doit                                            // Method name
  ( PerRow<X> pr, double ds[], PerRow<X> pr0 );   // Arguments for doit()


  public static class FlowFrame extends Flow {
    final Frame _fr;
    public FlowFrame( Frame fr ) { _fr = fr; }
    @Override Frame frame() { return _fr; }
    @Override public String toString() { return _fr.toString(); }
    @Override <X extends PerRow<X>> PerRow<X> doit(PerRow<X> pr, double ds[], PerRow<X> pr0) {
      if( pr == null ) pr = pr0.make();
      pr.mapreduce(ds); 
      return pr;
    }
  }

  public static class FlowFilter extends Flow {
    final Filter _fr;
    final Flow _ex;
    public FlowFilter( Filter fr, Flow ex ) { _fr = fr; _ex = ex;}
    public <Y extends PerRow<Y>> FlowPerRow<Y> with( PerRow<Y> pr ) {
      return new FlowPerRow<Y>(pr,this);
    }
    public FlowGroupBy with( GroupBy fr ) {
      return new FlowGroupBy(fr,this);
    }
    public FlowFilter with ( Filter filter){
      return new FlowFilter(filter, this);
    }
    @Override Frame frame() { return _ex.frame(); }
    @Override public String toString() { return _ex.toString()+".with("+_fr+")"; }
    @Override <X extends PerRow<X>> PerRow<X> doit(PerRow<X> pr, double ds[], PerRow<X> pr0) {
      return _fr.filter(ds) ? _ex.doit(pr,ds,pr0) : pr;
    }
  }

  public static class FlowGroupBy extends Flow {
    final GroupBy _gb;
    final Flow _ex;
    public FlowGroupBy( GroupBy gb, Flow ex ) { _gb = gb; _ex = ex;}
    public <Y extends PerRow<Y>> FlowGroupPerRow<Y> with( PerRow<Y> pr ) {
      return new FlowGroupPerRow<Y>(pr,this);
    }
    public FlowGroupBy with( Filter fr ) { 
      return new FlowGroupBy(_gb,new FlowFilter(fr,_ex));
    }
    @Override Frame frame() { return _ex.frame(); }
    @Override public String toString() { return _ex.toString()+".with("+_gb+")"; }
    @Override <X extends PerRow<X>> PerRow<X> doit(PerRow<X> pr, double ds[], PerRow<X> pr0) { throw H2O.fail(); }
  }

  public static class FlowGroupPerRow<X extends PerRow<X>> extends MRTask2<FlowGroupPerRow<X>> {
    final PerRow<X> _pr;        // Canonical example, not returned
    IcedHashMap<IcedLong,PerRow<X>> _prs;
    FlowGroupBy _ex;
    public FlowGroupPerRow( PerRow<X> pr, FlowGroupBy ex ) { 
      _pr = pr; 
      _ex = ex;
    }
    public IcedHashMap<IcedLong,X> doit() { return doAll(_ex.frame()).self(); }
    @Override public void map( Chunk chks[] ) {
      _prs = new IcedHashMap<IcedLong,PerRow<X>>();
      double ds[] = new double[chks.length];
      for( int i=0; i<chks[0]._len; i++ ) {
        // Load the internal double array
        for( int j=0; j<chks.length; j++ )
          ds[j] = chks[j].at0(i);
        IcedLong gid = new IcedLong(_ex._gb.groupId(ds));
        PerRow<X> pr1 = _prs.get(gid);
        PerRow<X> pr2 = _ex._ex.doit(pr1,ds,_pr);
        if( pr1 == null && pr2 != null ) _prs.put(gid,pr2);
      }
    }
    @Override public void reduce( FlowGroupPerRow<X> that ) {
      for( IcedLong gid : that._prs.keySet() ) {
        PerRow<X> that_pr = that._prs.get(gid);
        PerRow<X> this_pr = this._prs.get(gid);
        if( that_pr != null ) {
          if( this_pr != null ) this_pr.reduce((X)that_pr);
          else this._prs.put(gid,that_pr);
        }
      }
    }
    IcedHashMap<IcedLong,X> self() { return (IcedHashMap<IcedLong,X>)_prs; }
    @Override public String toString() { return _ex.toString()+".with("+_pr+")"; }
  }

  public static class FlowPerRow<X extends PerRow<X>> extends MRTask2<FlowPerRow<X>> {
    PerRow<X> _pr;
    Flow _ex;
    public FlowPerRow( PerRow<X> pr, Flow ex ) { _pr = pr; _ex = ex;}
    public X doit() { return doAll(_ex.frame()).self(); }
    @Override public void map( Chunk chks[] ) {
      _pr = _pr.make();
      double ds[] = new double[chks.length];
      for( int i=0; i<chks[0]._len; i++ ) {
        // Load the internal double array
        for( int j=0; j<chks.length; j++ )
          ds[j] = chks[j].at0(i);
        _ex.doit(_pr,ds,_pr);
      }
    }
    @Override public void reduce( FlowPerRow<X> ebpr ) { _pr.reduce(ebpr.self()); }
    X self() { return (X)_pr; }
    @Override public String toString() { return _ex.toString()+".with("+_pr+")"; }
  }

}
