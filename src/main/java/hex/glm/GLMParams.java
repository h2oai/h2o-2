package hex.glm;

import water.H2O;
import water.Iced;

public class GLMParams extends Iced {

  /**
   * passthrough class around family that properly supports icing
   */
  public static class FamilyIced extends Iced {
    public final double _tweedieVariancePower;
    public final Family _family;

    public FamilyIced( Family family ){
      _family = family;
      _tweedieVariancePower = Double.NaN;
    }
    public FamilyIced( Family family, double tweedieVariancePower ){
      _family = family;
      _tweedieVariancePower = tweedieVariancePower;
    }

    public double mustart(double y) {
      return _family.mustart(y);
    }
    public double variance(double mu) {
      switch( _family ){
        default:
          return _family.variance( mu );
        case tweedie:
          return Math.pow(mu, _tweedieVariancePower);
      }

    }
    public double deviance(double yr, double ym){
      switch( _family ){
        case gaussian:
        case binomial:
        case poisson:
        case gamma:
          return _family.deviance(yr,  ym);
        case tweedie:
          double one_minus_p = 1. - _tweedieVariancePower;
          double two_minus_p = 2. - _tweedieVariancePower;
          return Math.pow(yr, two_minus_p) / (one_minus_p * two_minus_p) - (yr * (Math.pow(ym, one_minus_p)))/one_minus_p + Math.pow(ym, two_minus_p)/two_minus_p;
        default:
          throw new RuntimeException("FamilyIced.deviance unknown family");
      }
    }

    public String toString2(){
      return String.format("FamilyIced(_family %s; tweedievp %f)", _family.toString2(), _tweedieVariancePower);
    }
  }

  // supported families
  public enum Family {
    gaussian(Link.identity, null), binomial(Link.logit, new double[] { Double.NaN, 1.0, 0.5 }), poisson(Link.log, null),
    gamma(Link.inverse, null), tweedie(Link.tweedie, null, Double.NaN);
    public Link defaultLink;
    public final double[] defaultArgs;
    public double tweedieVariancePower = Double.NaN;

    Family(Link l, double[] d) {
      defaultLink = l;
      defaultArgs = d;
    }

    Family(Link link, double[] d, double tweedieVariancePower){
      defaultLink = link;
      defaultArgs = d;
      this.tweedieVariancePower = tweedieVariancePower;
    }

    public double mustart(double y) {
      switch( this ) {
        case gaussian:
          return y;
        case binomial:
          return 0.5;
        case poisson:
          return y + 0.1;
        case gamma:
          return y;
        case tweedie:
          return y + (y==0. ? 0.1 : 0.);
        default:
          throw new RuntimeException("unimplemented");
      }
    }

    public double variance(double mu) {
      switch( this ) {
        case gaussian:
          return 1;
        case binomial:
          assert (0 <= mu && mu <= 1) : "mu out of bounds<0,1>:" + mu;
          return mu * (1 - mu);
        case poisson:
          return mu;
        case gamma:
          return mu * mu;
        case tweedie:
          return Math.pow(mu, tweedieVariancePower);
        default:
          throw new RuntimeException("unknown family Id " + this);
      }
    }

    /**
     * Per family deviance computation.
     *
     * @param family
     * @param yr
     * @param ym
     * @return
     */
    public double deviance(double yr, double ym) {
      switch( this ) {
        case gaussian:
          return (yr - ym) * (yr - ym);
        case binomial:
          return 2 * ((y_log_y(yr, ym)) + y_log_y(1 - yr, 1 - ym));
        case poisson:
          if( yr == 0 ) return 2 * ym;
          return 2 * ((yr * Math.log(yr / ym)) - (yr - ym));
        case gamma:
          if( yr == 0 ) return -2;
          return -2 * (Math.log(yr / ym) - (yr - ym) / ym);
        case tweedie:
          // Theory of Dispersion Models: Jorgensen
          // pg49: $$ d(y;\mu) = 2 [ y \cdot \left(\tau^{-1}(y) - \tau^{-1}(\mu) \right) - \kappa \{ \tau^{-1}(y)\} + \kappa \{ \tau^{-1}(\mu)\} ] $$
          // pg133: $$ \frac{ y^{2 - p} }{ (1 - p) (2-p) }  - \frac{y \cdot \mu^{1-p}}{ 1-p} + \frac{ \mu^{2-p} }{ 2 - p }$$
          double one_minus_p = 1. - tweedieVariancePower;
          double two_minus_p = 2. - tweedieVariancePower;
          return Math.pow(yr, two_minus_p) / (one_minus_p * two_minus_p) - (yr * (Math.pow(ym, one_minus_p)))/one_minus_p + Math.pow(ym, two_minus_p)/two_minus_p;

        default:
          throw new RuntimeException("unknown family Id " + this);
      }
    }

    public String toString2(){
      String s = "family(";
      switch(this){
        case gaussian: s += "gaussian: "; break;
        case binomial: s += "binomial: "; break;
        case gamma: s += "gamma: "; break;
        case poisson: s += "poisson: "; break;
        case tweedie: s += String.format("tweedie: variancePower %2.2f", this.tweedieVariancePower); break;
        default: s += "BAD UNKNOWN"; break;
      }
      s += String.format(", link: %s)", defaultLink);
      return s;
    }
  }

  public static enum Link {
    familyDefault(0), identity(0), logit(0), log(0.1),
    //    probit(0),
    //    cauchit(0),
    //    cloglog(0),
    //    sqrt(0),
    inverse(0),
    //    oneOverMu2(0);
    tweedie(0, Double.NaN /* default: 1. - 1.5 */)
    ;
    public final double defaultBeta;
    public double tweedieLinkPower;

    Link(double b) {
      defaultBeta = b;
    }
    Link(double b, double tweedieLinkPower){
      defaultBeta = b;
      this.tweedieLinkPower = tweedieLinkPower;
    }

    public final double link(double x) {
      switch( this ) {
        case identity:
          return x;
        case logit:
          assert 0 <= x && x <= 1;
          return Math.log(x / (1 - x));
        case log:
          return Math.log(x);
        case inverse:
          double xx = (x < 0) ? Math.min(-1e-5, x) : Math.max(1e-5, x);
          return 1.0 / xx;
        case tweedie:
          return Math.pow(x, tweedieLinkPower);
        default:
          throw new RuntimeException("unsupported link function id  " + this);
      }
    }

    public final double linkDeriv(double x) {
      switch( this ) {
        case logit:
          return 1 / (x * (1 - x));
        case identity:
          return 1;
        case log:
          return 1.0 / x;
        case inverse:
          return -1.0 / (x * x);
        case tweedie:
          return tweedieLinkPower * Math.pow(x, tweedieLinkPower - 1.);
        default:
          throw H2O.unimpl();
      }
    }

    public final double linkInv(double x) {
      switch( this ) {
        case identity:
          return x;
        case logit:
          return 1.0 / (Math.exp(-x) + 1.0);
        case log:
          return Math.exp(x);
        case inverse:
          double xx = (x < 0) ? Math.min(-1e-5, x) : Math.max(1e-5, x);
          return 1.0 / xx;
        case tweedie:
          return Math.pow(x, 1./tweedieLinkPower);
        default:
          throw new RuntimeException("unexpected link function id  " + this);
      }
    }

    public final double linkInvDeriv(double x) {
      switch( this ) {
        case identity:
          return 1;
        case logit:
          double g = Math.exp(-x);
          double gg = (g + 1) * (g + 1);
          return g / gg;
        case log:
          //return (x == 0)?MAX_SQRT:1/x;
          return Math.max(Math.exp(x), Double.MIN_NORMAL);
        case inverse:
          double xx = (x < 0) ? Math.min(-1e-5, x) : Math.max(1e-5, x);
          return -1 / (xx * xx);
        case tweedie:
          double vp = (1. - tweedieLinkPower) / tweedieLinkPower;
          return (1./tweedieLinkPower) * Math.pow(x, vp);
        default:
          throw new RuntimeException("unexpected link function id  " + this);
      }
    }

    public String toString2(){
      String s = "link(";
      switch(this){
        case identity:
          s += "identity: "; break;
        case logit:
          s += "logit: "; break;
        case log:
          s += "log: "; break;
        case inverse:
          s += "inverse: "; break;
        case tweedie:
          s += "tweedie: "; break;
        case familyDefault:
          s += "familyDefault: "; break;
        default:
          s+= " BAD DEFAULT: "; break;
      }
      s += String.format("defaultBeta: %f", defaultBeta);

      switch(this){
        case tweedie: s += String.format("; tweedieLinkPower: %2.2f", tweedieLinkPower); break;
        default: break;
      }

      s += ")";
      return s;
    }
  }

  // helper function
  static final double y_log_y(double y, double mu) {
    mu = Math.max(Double.MIN_NORMAL, mu);
    return (y != 0) ? (y * Math.log(y / mu)) : 0;
  }

  public enum CaseMode {
    none("n/a"), lt("<"), gt(">"), lte("<="), gte(">="), eq("="), neq("!="), ;
    final String _str;

    CaseMode(String str) {
      _str = str;
    }

    public String toString() {
      return _str;
    }

    public String exp(double v) {
      switch( this ) {
        case none:
          return "n/a";
        default:
          return "x" + _str + v;
      }
    }

    public final boolean isCase(double x, double y) {
      switch( this ) {
        case lt:
          return x < y;
        case gt:
          return x > y;
        case lte:
          return x <= y;
        case gte:
          return x >= y;
        case eq:
          return x == y;
        case neq:
          return x != y;
        default:
          assert false;
          return false;
      }
    }
  }



}
