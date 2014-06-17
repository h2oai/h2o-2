package water.api.anno;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
public @interface RESTCall {
  /** Schema location */
  String location();
  /** Endpoint */
  String path();
  /** Call method */
  String method() default "GET";
}
