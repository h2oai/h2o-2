package water.exec;
import water.Key;

/**
 * Execute a generic R string, in the context of an H2O Cloud
 *
 * @author cliffc@0xdata.com
 */
public class Exec {
  // Execute some generic R string.  Return a
  public static Key exec( String x ) throws ParserException, EvaluationException {
    return exec(x,"Result.hex");
  }
  
  public static Key exec(String x, String to) throws ParserException, EvaluationException {
    Key k = Key.make(to);
    Expr e = new RLikeParser().parse(x);
    Expr.Result r = e.eval();
    Helpers.assign(0,k, r);
    Helpers.calculateSigma(k,0);
    r.dispose();
    return k;
  }
}