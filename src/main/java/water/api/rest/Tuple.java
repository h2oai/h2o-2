package water.api.rest;

public class Tuple<T1, T2> {
  public Tuple(T1 f, T2 l) { this.f = f; this.l = l; }
  final T1 f;
  final T2 l;
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((f == null) ? 0 : f.hashCode());
    result = prime * result + ((l == null) ? 0 : l.hashCode());
    return result;
  }
  @Override public boolean equals(Object obj) {
    if( this == obj )
      return true;
    if( obj == null )
      return false;
    if( getClass() != obj.getClass() )
      return false;
    Tuple other = (Tuple) obj;
    if( f == null ) {
      if( other.f != null )
        return false;
    } else if( !f.equals(other.f) )
      return false;
    if( l == null ) {
      if( other.l != null )
        return false;
    } else if( !l.equals(other.l) )
      return false;
    return true;
  }
}