package water.api;

import java.lang.annotation.*;

import water.api.Request.Filter;
import water.api.Request.Validator;
import water.api.Request.Validator.NOPValidator;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@Documented
public @interface API {
  String help();
  /** Must be specified. */
  boolean required() default false;
  /** For keys. If specified, the key must exist. */
  boolean mustExist() default false;
  int since() default 1;
  int until() default Integer.MAX_VALUE;
  Class<? extends Filter> filter() default Filter.class;
  Class<? extends Filter>[] filters() default {};
  /** Forces an input field to also appear in JSON. */
  boolean json() default false;
  long   lmin() default Long  .MIN_VALUE;
  long   lmax() default Long  .MAX_VALUE;
  double dmin() default Double.NEGATIVE_INFINITY;
  double dmax() default Double.POSITIVE_INFINITY;
  boolean hide() default false;
  String displayName() default "";
  boolean gridable() default true;
  Class<? extends Validator> validator() default Validator.NOPValidator.class;
  ParamImportance importance() default ParamImportance.EXPERT; // Show show up in UI by default.

  // ============
  // NEW API
  String[] dependsOn() default {}; // Should be field automatically depending on values, valid fields
  String[] helpFiles() default {};
  Direction direction() default Direction.OUT;
  /** REST path to reference of this field */
  String path() default "";
  /** Validation String for annotated field - make sense only for input annotation!
   * It should express a predicate, e.g.:
   * <code>"/frames/${/parameters/source}/cols/${/parameters/response}/type != 'Float' && ${/parameters/learn_rate} > 1000</code> */
  String valid() default "";
  /** Is the field is enabled. */
  String enabled() default "";
  /** Is the field visibled. */
  String visible() default "";
  /** Predefined values for the field - can be a list of values, or query to
   * obtain values.
   *
   * <code>/frames/${source}/cols?names</code>, <code>1,2,10,15</code>
   */
  String values() default "";
  /**
   * Type of parameter
   */
  Class type() default Void.class;

  // =========
}