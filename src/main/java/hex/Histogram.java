package hex;

import com.google.gson.JsonObject;
import water.*;

public abstract class Histogram {

   static public JsonObject run(ValueArray ary, int col) {
      // Pass 1: prepares outline
      long start = System.currentTimeMillis();
      OutlineTask t1 = new OutlineTask();
      t1._arykey = ary._key;
      t1._col = col;
      t1.invoke(ary._key);
      long pass1 = System.currentTimeMillis();
      
      // Uses Scott's formula
      double sd = Math.sqrt(t1._sumsq/t1._n - 
                            (t1._sum/t1._n) * (t1._sum/t1._n));
      double binsz = 3.5 * sd / Math.cbrt(t1._n);
      int nbin = (int)((t1._max - t1._min) / binsz);
      Bins bins = Bins.makePrettyBins(t1._min, t1._max, nbin);
      BinningTask t2 = new BinningTask();
      t2._hist = bins;
      t2._arykey = ary._key;
      t2._col = col;
      t2.invoke(ary._key);
      long pass2 = System.currentTimeMillis();

      JsonObject res = new JsonObject();
      res.addProperty("Key", ary._key.toString());
      res.addProperty("Pass1Msecs", pass1 - start);
      res.addProperty("Pass2Msecs", pass2 - pass1);
      res.addProperty("Rows", t1._n);
      res.addProperty("SD", sd);
      res.addProperty("Hist_Start", bins._start);
      res.addProperty("Hist_End", bins._end);
      res.addProperty("Hist_Bins", bins._bins.length);
      for (int i = 0; i < bins._bins.length; i++)
         res.addProperty("Bin_" + i, bins._bins[i]);
      return res;
   }

   static class Bins extends Iced {
      long[] _bins;
      double _start, _end, _binsz;
      
      private Bins() {}
      
      Bins(double start, double binsz, int nbin) {
         assert binsz > 0;
         assert nbin > 0;
         assert nbin * binsz < Double.MAX_VALUE;
         _bins = new long[nbin];
         _start = start;
         _binsz = binsz;
         _end = start + nbin * binsz;
      }
      
      public AutoBuffer write(AutoBuffer bb) { 
         bb.putA8(_bins);
         bb.put8d(_start);
         bb.put8d(_end);
         bb.put8d(_binsz);
         return bb;
      }
      public Bins read(AutoBuffer bb) {
         Bins bins = new Bins();
         bins._bins = bb.getA8();
         bins._start = bb.get8d();
         bins._end = bb.get8d();
         bins._binsz = bb.get8d();
         return bins;
      }
      void union(Bins other) {
         assert _bins.length == other._bins.length;
         assert Math.abs(_start - other._start) < 0.000001;
         assert Math.abs(_binsz - other._binsz) < 0.000000001;
         for (int i = 0; i < _bins.length; i++)
            _bins[i] += other._bins[i];
      }

      void add(double val) {
         double di = (val - _start) / _binsz;
         int i = di < 0.0 ? 0 : 
            di > (double)_bins.length ? _bins.length - 1 : (int)di;
         _bins[i]++;
      }

      public static Bins makePrettyBins(double start, double end, int n) {
         assert n >= 0;
         n = n == 0 ? 1 : n;
         double c = (end - start) / n;
         double d = Math.pow(10, Math.floor(Math.log10(c)));
         // selects among d, 5*d, and 10*d so that the number of
         // partitions go in [start, end] is closest to n
         if (c > 20*d/3)
            d *= 10;
         else if (c > 5*d/3)
            d *= 5;
         start = d * Math.floor(start / d);
         end = d * Math.ceil(end / d);
         n = (int)((end - start) / d);
         Bins bins = new Bins(start, d, n);
         return bins;
      }
   }

   public static class OutlineTask extends MRTask {
      Key _arykey;
      int _col;
      long _n = 0;
      double _sum = .0, _sumsq = .0;
      double _min = Double.POSITIVE_INFINITY;
      double _max = Double.NEGATIVE_INFINITY;

      public void map(Key key) {
         assert key.home();
         // Get the root ValueArray for the metadata
         ValueArray ary = DKV.get(_arykey).get();
         // Get the raw bits to work on
         AutoBuffer bits = ary.getChunk(key);
         final int nrow = bits.remaining()/ary._rowsize;
         ValueArray.Column col = ary._cols[_col];

         int nbad = 0;
         if (!ary.hasInvalidRows(_col)) {
            for (int i = 0; i < nrow; i++) {
               double v = ary.datad(bits, i, col);
               _sum   += v;
               _sumsq += v * v;
               if (v < _min)
                  _min = v;
               if (v > _max)
                  _max = v;
            }
         } else {
            for (int i = 0; i < nrow; i++) {
               if (ary.isNA(bits, i, col)) {
                  ++nbad;
               } else {
                  double v = ary.datad(bits, i, col);
                  _sum   += v;
                  _sumsq += v * v;
                  if (v < _min)
                     _min = v;
                  if (v > _max)
                     _max = v;
               }
            }
         }
         _n = (long)(nrow - nbad);
      }

      public void reduce(DRemoteTask rt) {
         OutlineTask other = (OutlineTask)rt;
         _n     += other._n;
         _sum   += other._sum;
         _sumsq += other._sumsq;
         _min    = other._min < _min ? other._min : _min;
         _max    = other._max > _max ? other._max : _max;
      }
   }

   public static class BinningTask extends MRTask {
      Key _arykey; // Main ValueArray key
      int _col; // Which columns to work on
      Bins _hist;
      
      public void map(Key key) {
         assert key.home();
         // Get the root ValueArray for the metadata
         ValueArray ary = DKV.get(_arykey).get();
         // Get the raw bits to work on
         AutoBuffer bits = ary.getChunk(key);
         final int nrow = bits.remaining()/ary._rowsize;
         ValueArray.Column col = ary._cols[_col];

         if (!ary.hasInvalidRows(_col)) {
            for (int i = 0; i < nrow; i++) {
               double v = ary.datad(bits, i, col);
               _hist.add(v);
            }
         } else {
            for (int i = 0; i < nrow; i++) {
               if (!ary.isNA(bits, i, col)) {
                  double v = ary.datad(bits, i, col);
                  _hist.add(v);
               }
            }
         }
      }

      public void reduce(DRemoteTask rt) {
         BinningTask other = (BinningTask)rt;
         _hist.union(other._hist);
      }
   }
}