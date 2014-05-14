package water.api.rest;

import java.lang.reflect.Field;

import water.H2O;

public class RestUtils {

  public static boolean fillField(Object source, String ffrom, Object target, String fto, Object valueTransformers) {
    try {
      Field ff = source.getClass().getField(ffrom);
      Field ft = target.getClass().getField(fto);
      if (ft.getType().isAssignableFrom(ff.getType())) {
        ft.set(target, ff.get(source));
        return true;
      } else {
        // Find the right value transformer and transform the value.
        throw H2O.unimpl();
        //return false;
      }
    } catch (Throwable t) {
      throw new IllegalArgumentException("Cannot transfer field: " + ffrom + " from: " + source + " to field " + fto + " on " +target, t);
    }
  }
}
