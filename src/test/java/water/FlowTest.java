package water;

import static org.junit.Assert.*;
import org.junit.*;
import water.fvec.*;
import water.nbhm.*;

public class FlowTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // ---
  // Test flow-coding a filter & group-by computing e.g. mean
  @Test public void testBasic() {
    Key k = Key.make("cars.hex");
    try {
      Frame fr = parseFrame(k, "smalldata/cars.csv");
      final int cyl_idx = fr.find("cylinders");
      final int year_idx = fr.find("year");
    
      SumCol sumcols = fr.
        with(new SumCol(year_idx)).
        doit();
      System.out.println(sumcols._sum+"/"+sumcols._n+" = "+(sumcols._sum/sumcols._n));
      
      SumCol sumcols1 = fr.
        with(new FilterRow() { boolean filter(double ds[]) { return ds[cyl_idx]!=5; } }).
        with(new SumCol(year_idx)).
        doit();
      System.out.println(sumcols1._sum+"/"+sumcols1._n+" = "+(sumcols1._sum/sumcols1._n));
      
      NonBlockingHashMapLong<SumCol> sumcols2 = fr.
        with(new GroupBy() { long groupId(double ds[]) { return (long)ds[cyl_idx];} }).
        with(new SumCol(year_idx)).
        doit();
      
      //NonBlockingHashMapLong<SumCol> sumcols2 = fr.
      //  with(new FilterRow() { boolean filter(double ds[]) { return ds[cyl_idx]!=5; }}).
      //  with(new GroupBy() { long groupId(double ds[]) { return (long)ds[cyl_idx];} }).
      //  with(new SumCol(year_idx)).
      //  doit();
      //
      //NonBlockingHashMapLong<SumCol> sumcols3 = fr.
      //  with(new GroupBy() { long groupId(double ds[]) { return (long)ds[cyl_idx];} }).
      //  with(new FilterRow() { boolean filter(double ds[]) { return ds[cyl_idx]!=5; }}).
      //  with(new SumCol(year_idx)).
      //  doit();

    } finally {
      UKV.remove(k);
    }
  }

  private static class SumCol extends PerRow<SumCol> {
    final int _col_idx;
    double _sum, _n;
    SumCol( int col_idx  ) { _col_idx = col_idx; }
    @Override public void mapreduce( double ds[] ) { _sum += ds[_col_idx]; _n++; }
    @Override public void reduce( SumCol sy ) { _sum += sy._sum; _n += sy._n; }
  }

  public abstract static class GroupBy {
    abstract long groupId( double ds[]);
  }

  // -----------------------
  public abstract static class PerRow<X extends PerRow> extends Iced {
    abstract void mapreduce( double ds[] );
    abstract void reduce( X that );
    @Override public String toString() { return "perRow"; }
  }

  public abstract static class FilterRow extends Iced {
    abstract boolean filter( double ds[] );
    @Override public String toString() { return "filter"; }
  }

  public abstract static class ExecBuilder extends Iced {
    abstract Frame frame();
    abstract void doit(PerRow pr, double ds[]);
  }

  public static class ExecBuilderFrame extends ExecBuilder {
    final Frame _fr;
    public ExecBuilderFrame( Frame fr ) { _fr = fr; }
    @Override Frame frame() { return _fr; }
    @Override public String toString() { return _fr.toString(); }
    @Override void doit(PerRow pr, double ds[]) { pr.mapreduce(ds); }
  }

  public static class ExecBuilderFilter extends ExecBuilder {
    final FilterRow _fr;
    final ExecBuilder _ex;
    public ExecBuilderFilter( FilterRow fr, ExecBuilder ex ) { _fr = fr; _ex = ex;}
    public <Y extends PerRow<Y>> ExecBuilderPerRow<Y> with( PerRow<Y> pr ) {
      return new ExecBuilderPerRow<Y>(pr,this);
    }
    @Override Frame frame() { return _ex.frame(); }
    @Override public String toString() { return _ex.toString()+".with("+_fr+")"; }
    @Override void doit(PerRow pr, double ds[]) {
      if( _fr.filter(ds) ) pr.mapreduce(ds); 
    }
  }

  public static class ExecBuilderGroupBy extends ExecBuilder {
    final GroupBy _gb;
    final ExecBuilder _ex;
    public ExecBuilderGroupBy( GroupBy gb, ExecBuilder ex ) { _gb = gb; _ex = ex;}
    public <Y extends PerRow<Y>> ExecBuilderPerRow<Y> with( PerRow<Y> pr ) {
      return new ExecBuilderPerRow<Y>(pr,this);
    }
    public ExecBuilder with( FilterRow fr ) { return new ExecBuilderFilter(fr,this);  }
    @Override Frame frame() { return _ex.frame(); }
    @Override public String toString() { return _ex.toString()+".with("+_gb+")"; }
    @Override void doit(PerRow pr, double ds[]) {
      throw H2O.unimpl();
    }
  }

  public static class ExecBuilderPerRow<X extends PerRow<X>> extends MRTask2<ExecBuilderPerRow<X>> {
    final PerRow<X> _pr;
    ExecBuilder _ex;
    public ExecBuilderPerRow( PerRow<X> pr, ExecBuilder ex ) { _pr = pr; _ex = ex;}
    public X doit() { 
      System.out.println(toString());
      return doAll(_ex.frame()).self(); 
    }
    @Override public void map( Chunk chks[] ) {
      double ds[] = new double[chks.length];
      for( int i=0; i<chks[0]._len; i++ ) {
        // Load the internal double array
        for( int j=0; j<chks.length; j++ )
          ds[j] = chks[j].at0(i);
        _ex.doit(_pr,ds);
      }
    }
    @Override public void reduce( ExecBuilderPerRow<X> ebpr ) { _pr.reduce(ebpr.self()); }
    X self() { return (X)_pr; }
    @Override public String toString() { return _ex.toString()+".with("+_pr+")"; }
  }

}
