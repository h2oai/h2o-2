package hex.glm;


import hex.FrameTask;
import water.H2O;
import water.Iced;
import water.MemoryManager;
import water.api.DocGen;
import water.api.Request.API;

public final class GLMParams extends Iced {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="family")
  final Family family;
  @API(help="link")
  final Link   link;
  @API(help="tweedie variance power")
  final double tweedie_variance_power;
  @API(help="tweedie link power")
  final double tweedie_link_power;

  public GLMParams(Family f){this(f,0,f.defaultLink,0);}
  public GLMParams(Family f,Link l){this(f,0,l,0);}
  public GLMParams(Family f, double twVar){this(f,twVar,f.defaultLink,1-twVar);}
  public GLMParams(Family f, double twVar, Link l, double twLnk){
    family = f;
    if(l == Link.family_default)
      link = family.defaultLink;
    else { // check we have compatible link
      link = l;
      switch(family){
        case gaussian:
          if(link != Link.identity && link != Link.log && link != Link.inverse)
            throw new IllegalArgumentException("Incompatible link function for selected family. Only identity, log and inverse links are allowed for family=gaussian.");
          break;
        case binomial:
          if(link != Link.logit && link != Link.log)
            throw new IllegalArgumentException("Incompatible link function for selected family. Only logit and log links are allowed for family=binomial.");
          break;
        case poisson:
          if(link != Link.log && link != Link.identity)
            throw new IllegalArgumentException("Incompatible link function for selected family. Only log and identity links are allowed for family=poisson.");
          break;
        case gamma:
          if(link != Link.inverse && link != Link.log && link != Link.identity)
            throw new IllegalArgumentException("Incompatible link function for selected family. Only inverse, log and identity links are allowed for family=gamma.");
          break;
        case tweedie:
          if(link != Link.tweedie)
            throw new IllegalArgumentException("Incompatible link function for selected family. Only tweedie link allowed for family=tweedie.");
          break;
        default:
          H2O.fail();
      }
    }
    tweedie_variance_power = twVar;
    tweedie_link_power = twLnk;
  }

  public final double variance(double mu){
    switch( family ) {
      case gaussian:
        return 1;
      case binomial:
//        assert (0 <= mu && mu <= 1) : "mu out of bounds<0,1>:" + mu;
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

  public final boolean canonical(){
    switch(family){
      case gaussian:
        return link == Link.identity;
      case binomial:
        return link == Link.logit;
      case poisson:
        return link == Link.log;
      case gamma:
        return false; //return link == Link.inverse;
      case tweedie:
        return false;
      default:
        throw H2O.unimpl();
    }
  }

  public final double mustart(double y, double ymu) {
    switch( family ) {
      case gaussian:
      case binomial:
      case poisson:
        return ymu;
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
        assert 0 <= x && x <= 1:"x out of bounds, expected <0,1> range, got " + x;
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
    double res = 0;
    switch( link ) {
      case logit:    res = +1.0 / (x * (1 - x)); break;
      case identity: res = +1.0                ; break;
      case log:      res = +1.0 / x            ; break;
      case inverse:  res = -1.0 / (x * x)      ; break;
      case tweedie:
        res = tweedie_link_power * Math.pow(x, tweedie_link_power - 1);
        break;
      default:
        throw H2O.unimpl();
    }
    if(res > 1e5)
      res = 1e5;
    else if(res < -1e5)
      res = -1e5;
    return res;
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
  public static enum Link {family_default, identity, logit, log,inverse,tweedie;}

  // helper function
  static final double y_log_y(double y, double mu) {
    if(y == 0)return 0;
    if(mu < Double.MIN_NORMAL) mu = Double.MIN_NORMAL;
    return y * Math.log(y / mu);
  }

 public Family getFamily() {
   return family;
 }

}
