package hex;

import water.ValueArray;

public final class ColSummaryTask extends RowTask<Summary>{
  final int [] _cols;
  public ColSummaryTask(ValueArray ary, int [] cols){super(ary);_cols = cols;}
  @Override public Summary newRes() {return new Summary(_ary,_cols);}

  @Override public void map(hex.RowTask.Row r, Summary summary) {
    for(int i = 0; i < _cols.length; ++i) {
      if(!r.isNA(_cols[i]))
        summary._sums[i].add(r.getDCol(_cols[i]));
      else
        summary._sums[ i ]._n_na ++;
    }
  }
  @Override public Summary reduce(Summary left, Summary right) {
    return left.add(right);
  }
}