package hex.glm;


import water.H2O;
import water.Iced;

public class GLMParams extends Iced {
  final Family family;
  final Link   link;
  final double tweedie_variance_power;
  final double tweedie_link_power;

  public GLMParams(Family f, double twVar, Link l, double twLnk){
    family = f;
    link = l;
    tweedie_variance_power = twVar;
    tweedie_link_power = twLnk;
  }

  public final double variance(double mu){
    switch( family ) {
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
        return Math.pow(mu, tweedie_variance_power);
      default:
        throw new RuntimeException("unknown family Id " + this);
    }
  }

  public final double mustart(double y) {
    switch( family ) {
      case gaussian:
        return y;
      case binomial:
        return 0.5;
      case poisson:
        return y + 0.1;
      case gamma:
        return y;
      case tweedie:
        return y + (y==0?0.1:0);
      default:
        throw new RuntimeException("unimplemented");
    }
  }

  public final double deviance(double yr, double ym){
    switch(family){
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
        double one_minus_p = 1 - tweedie_variance_power;
        double two_minus_p = 2 - tweedie_variance_power;
        return Math.pow(yr, two_minus_p) / (one_minus_p * two_minus_p) - (yr * (Math.pow(ym, one_minus_p)))/one_minus_p + Math.pow(ym, two_minus_p)/two_minus_p;
      default:
        throw new RuntimeException("unknown family " + family);
    }
  }

  public final double link(double x) {
    switch( link ) {
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
        return Math.pow(x, tweedie_link_power);
      default:
        throw new RuntimeException("unknown link function " + this);
    }
  }

  public final double linkDeriv(double x) {
    switch( link ) {
      case logit:
        return 1 / (x * (1 - x));
      case identity:
        return 1;
      case log:
        return 1.0 / x;
      case inverse:
        return -1.0 / (x * x);
      case tweedie:
        return tweedie_link_power * Math.pow(x, tweedie_link_power - 1);
      default:
        throw H2O.unimpl();
    }
  }

  public final double linkInv(double x) {
    switch( link ) {
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
        return Math.pow(x, 1/tweedie_link_power);
      default:
        throw new RuntimeException("unexpected link function id  " + this);
    }
  }

  public final double linkInvDeriv(double x) {
    switch( link ) {
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
        double vp = (1. - tweedie_link_power) / tweedie_link_power;
        return (1/tweedie_link_power) * Math.pow(x, vp);
      default:
        throw new RuntimeException("unexpected link function id  " + this);
    }
  }

  // supported families
  public enum Family {
    gaussian(Link.identity), binomial(Link.logit), poisson(Link.log),
    gamma(Link.inverse), tweedie(Link.tweedie);
    public final Link defaultLink;
    Family(Link link){defaultLink = link;}
  }
  public static enum Link {identity, logit, log,inverse,tweedie;}

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
