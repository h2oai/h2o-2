package water.exec;

/**
 *
 * @author peta
 */
public class PositionedException extends Exception {

  public final int _pos;

  protected PositionedException(int pos, String msg) {
    super(msg);
    _pos = pos;
  }

  public String report(String source) {
    StringBuilder sb = new StringBuilder();
    sb.append(source);
    sb.append("\n");
    for( int i = 0; i < _pos; ++i ) {
      sb.append(' ');
    }
    sb.append("^\n");
    sb.append(this.toString());
    return sb.toString();
  }

  public String reportHTML(String source) {
    StringBuilder sb = new StringBuilder();
    sb.append(source);
    sb.append("<br/>");
    for( int i = 0; i < _pos; ++i ) {
      sb.append("&nbsp;");
    }
    sb.append("^<br/>");
    sb.append(this.toString());
    return sb.toString();
  }
}
