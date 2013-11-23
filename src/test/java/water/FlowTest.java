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

      SumCol sumcols1 = fr.
        with(new FilterRow() { boolean filter(double ds[]) { return ds[cyl_idx]!=5; }}).
        with(new SumCol(year_idx)).
        doit();


      NonBlockingHashMapLong<SumCol> sumcols2 = fr.
        with(new FilterRow() { boolean filter(double ds[]) { return ds[cyl_idx]!=5; }}).
        with(new GroupBy() { long groupId(double ds[]) { return (long)ds[cyl_idx];} }).
        with(new SumCol(year_idx)).
        doit();

      NonBlockingHashMapLong<SumCol> sumcols3 = fr.
        with(new GroupBy() { long groupId(double ds[]) { return (long)ds[cyl_idx];} }).
        with(new FilterRow() { boolean filter(double ds[]) { return ds[cyl_idx]!=5; }}).
        with(new SumCol(year_idx)).
        doit();


      System.out.println(sumcols._sum+"/"+sumcols._n+" = "+(sumcols._sum/sumcols._n));

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
  public abstract static class PerRow<X extends PerRow<X>> extends MRTask2<PerRow<X>> {
    @Override public void map( Chunk chks[] ) {
      double ds[] = new double[chks.length];
      for( int i=0; i<chks[0]._len; i++ ) {
        // Load the internal double array
        for( int j=0; j<chks.length; j++ )
          ds[j] = chks[j].at0(i);
        mapreduce(ds);
      }
    }
    abstract void mapreduce( double ds[] );
    abstract void reduce( X that );
    X self() { return (X)this; }
  }

  public abstract static class FilterRow {
    abstract boolean filter( double ds[] );
  }

  public static class ExecBuilder {
    final FilterRow _filt;
    final Frame _fr;
    public ExecBuilder( Frame fr, FilterRow filt ) { _fr = fr; _filt = filt; }
    public <Y extends FlowTest.PerRow<Y>> FlowTest.ExecBuilderPerRow<Y> with( FlowTest.PerRow<Y> pr ) {
      return new FlowTest.ExecBuilderPerRow<Y>(null,pr);
    }
    public <Y extends FlowTest.PerRow<Y>> FlowTest.ExecBuilderPerRow<Y> with( GroupBy ) {
      return null;
    }
  }

  // -----------------------
  public static class ExecBuilderPerRow<X extends PerRow<X>> {
    final PerRow<X> _pr;
    final Frame _fr;
    public ExecBuilderPerRow( Frame fr, PerRow<X> pr ) { _fr = fr; _pr = pr; }
    public X doit() { return _pr.doAll(_fr).self(); }
  }
}
