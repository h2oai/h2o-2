package water.slice;

import water.Iced;
import water.MRTask2;
import water.fvec.Chunk;
import water.util.Utils;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A slice task is a collective of map-reduce tasks each applied to a slice
 * of the data frame. Slicing rule is supplied by a {@code Slicer} object.
 */
abstract public class SliceTask<K extends SliceKey, S extends Slice> extends MRTask2<SliceTask<K,S>> {
  public static class RowView implements Iterable<RowView> {
    private Chunk[] _cs;
    private int     _i0;
    public RowView(Chunk[] cs) {
      _cs = cs;
    }
    // Accessor
    public double  getDouble(int col) { return _cs[col].at0  (_i0); }
    public long    getLong  (int col) { return _cs[col].at80 (_i0); }
    public boolean isNA     (int col) { return _cs[col].isNA0(_i0); }
    // iterator
    @Override public Iterator<RowView> iterator() {
      return new Iterator<RowView>() {
        private int _i0 = 0;
        @Override public boolean hasNext() { return _i0 < _cs[0]._len; }
        @Override public RowView next() { RowView.this._i0 = _i0; _i0++; return RowView.this; }
        @Override public void remove()  { throw new UnsupportedOperationException("Removing row is not supported."); }
      };
    }
  }

  abstract public static class Slicer<K extends SliceKey> extends Iced {
    abstract public K slice(RowView rv);
  }

  // Override these two methods to complete a map-reduce algorithm over a slice.
  // Intermediate state should be maintained in the slice data structure so as to be well isolated.
  abstract S addRow( S slice, RowView rv );
  abstract S reduce( S s1, Slice s2 );
  //
  private Slicer<K> _slicer;
  // Isolated workspaces
  private LinkedBlockingQueue<Workspace<K,S>> _wsq;
  public SliceTask(LinkedBlockingQueue<Workspace<K, S>> wsq, Slicer slicer) {
    _wsq = wsq; _slicer = slicer;
  }
  @Override public void map(Chunk[] cs) {
    Workspace<K,S>    ws = null;
    RowView           rv;
    Iterator<RowView> ri;
    try {
      ws = _wsq.take();
    } catch (InterruptedException e) {
      System.exit(-1);
    }
    rv = new RowView(cs);
    ri = rv.iterator();
    while (ri.hasNext()) {
      rv = ri.next();
      addRow(ws.getSlice(_slicer.slice(rv)), rv);
    }
    try {
      _wsq.put(ws);
    } catch (InterruptedException e) {
      System.exit(-1);
    }
  }
  @Override protected void postGlobal() {
    Workspace<K,S> ws = _wsq.poll();
    if (ws == null) throw new RuntimeException("Workspace queue should not be empty.");
      while (!_wsq.isEmpty()) {
        ws.merge(_wsq.poll());
      }
  }
}

