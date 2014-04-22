package water;

import org.junit.Ignore;
import org.junit.Test;

import water.fvec.Frame;
import water.exec.Flow;
import water.util.Utils.*;
import hex.Summary2.SummaryPerRow;

// Called from FlowTest, and is NOT a JUnit - so instances of this class will
// be lazily generated, so we do not need any TypeMap ID's before the H2O is up.
public class FlowTest2 {

  // Use a static method so all my anonymous inner classes do not carry a hidden
  // link to a FlowTest object.
  public static void basicStatic( Key k, Frame fr ) {
    try {
      final int cyl_idx = fr.find("cylinders");
      final int year_idx = fr.find("year");
      //final int cyl_idx = fr.find("C54"); // Works great for finding covtype - average elevation by class
      //final int year_idx = fr.find("C0");

      SumCol sumcols = fr.
        with(new SumCol(year_idx)).
        doit();
      System.out.println(sumcols._sum+"/"+sumcols._n+" = "+(sumcols._sum/sumcols._n));
      System.out.println();

      SumCol sumcols1 = fr.
        with(new Flow.Filter() { public boolean filter(double ds[]) { return ds[cyl_idx]!=5; } }).
        with(new SumCol(year_idx)).
        doit();
      System.out.println(sumcols1._sum+"/"+sumcols1._n+" = "+(sumcols1._sum/sumcols1._n));
      System.out.println();

      // Run all the rollups in parallel before doing summary
      Futures fs = new Futures();
      for( int i=0; i<fr.numCols(); i++ )
        fr.vecs()[i].rollupStats(fs);
      fs.blockForPending();

      IcedHashMap<IcedLong,SumCol> sumcols2 = fr.
        with(new Flow.GroupBy() { public long groupId(double ds[]) { return (long)ds[cyl_idx];} }).
        with(new SumCol(year_idx)).
        doit();
      for( IcedLong gid : sumcols2.keySet() ) {
        SumCol sumcol = sumcols2.get(gid);
        System.out.println("Cyl="+gid._val+", "+sumcol._sum+"/"+sumcol._n+" = "+(sumcol._sum/sumcol._n));
      }
      System.out.println();

      IcedHashMap<IcedLong,SumCol> sumcols3 = fr.
        with(new Flow.Filter () { public boolean filter(double ds[]) { return ds[cyl_idx]!=5; } }).
        with(new Flow.GroupBy() { public long groupId ( double ds[]) { return (long)ds[cyl_idx];} }).
        with(new SumCol(year_idx)).
        doit();
      for( IcedLong gid : sumcols3.keySet() ) {
        SumCol sumcol = sumcols3.get(gid);
        System.out.println("Cyl="+gid._val+", "+sumcol._sum+"/"+sumcol._n+" = "+(sumcol._sum/sumcol._n));
      }
      System.out.println();

      IcedHashMap<IcedLong,SumCol> sumcols4 = fr.
        with(new Flow.GroupBy() { public long groupId(double ds[]) { return (long)ds[cyl_idx];} }).
        with(new Flow.Filter() { public boolean filter(double ds[]) { return ds[cyl_idx]!=5; } }).
        with(new SumCol(year_idx)).
        doit();
      for( IcedLong gid : sumcols4.keySet() ) {
        SumCol sumcol = sumcols4.get(gid);
        System.out.println("Cyl="+gid._val+", "+sumcol._sum+"/"+sumcol._n+" = "+(sumcol._sum/sumcol._n));
      }
      System.out.println();

      // Percentiles
      SummaryPerRow spr = fr.
        with(new SummaryPerRow(fr)).
        doit();
      spr.finishUp();
      System.out.println(spr);
      System.out.println();

      // Percentiles per-Group
      IcedHashMap<IcedLong,SummaryPerRow> sprs = fr.
        with(new Flow.GroupBy() { public long groupId(double ds[]) { return (long)ds[cyl_idx];} }).
        with(new SummaryPerRow(fr)).
        doit();
      for( IcedLong gid : sprs.keySet() ) {
        SummaryPerRow spr2 = sprs.get(gid);
        spr2.finishUp();
        System.out.println("Group ID="+gid._val);
        System.out.println(spr2);
        System.out.println();
      }
      System.out.println();


    } finally {
      Lockable.delete(k);
    }
  }

  public static class SumCol extends Flow.PerRow<SumCol> {
    final int _col_idx;
    double _sum, _n;
    SumCol( int col_idx  ) { _col_idx = col_idx; }
    @Override public void mapreduce( double ds[] ) { _sum += ds[_col_idx]; _n++; }
    @Override public void reduce( SumCol that ) { _sum += that._sum; _n += that._n; }
    @Override public SumCol make() { return new SumCol(_col_idx); }
  }

  @Test @Ignore public void dummy_test() {
    /* this is just a dummy test to avoid JUnit complains about missing test */
  }
}
