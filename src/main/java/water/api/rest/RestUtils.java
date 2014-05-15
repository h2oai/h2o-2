package water.api.rest;

import java.lang.reflect.Field;
import java.util.Map;

import water.Iced;
import water.api.Request.API;
import water.api.Request.Direction;
import water.api.rest.REST.RestCall;
import water.api.rest.REST.TransfSig;
import water.api.rest.REST.ValueTransf;

public class RestUtils {
  static final Class [] p(Class a, Class b) { return new Class[] { a, b}; }
  static final String[] p(String a, String b) { return new String[] { a, b }; }
  static final String[][] map(String[] ...s)  { return s; }
  public static TransfSig tsig(Class a, Class b) { return new TransfSig(a, b); }

  public static <A extends RestCall, I extends Iced> boolean fillAPIField(I source, String ffrom, A target, String fto, Map<TransfSig, ValueTransf> valueTransformers) {
    return fillField(source, ffrom, target, fto, valueTransformers, false);
  }
  public static <A extends RestCall, I extends Iced> boolean fillImplField(A source, String ffrom, I target, String fto, Map<TransfSig, ValueTransf> valueTransformers) {
    return fillField(source, ffrom, target, fto, valueTransformers, true);
  }
  public static boolean fillField(Object source, String ffrom, Object target, String fto, Map<TransfSig, ValueTransf> valueTransformers, boolean direction /* true: source == API, false: target == API */) {
    try {
      Field ff = field(source.getClass(),ffrom); ff.setAccessible(true);
      Field ft = field(target.getClass(),fto); ft.setAccessible(true);
      API apiano = direction ? ff.getAnnotation(API.class) : ft.getAnnotation(API.class);
      if (apiano==null) throw new IllegalArgumentException("Cannot transfer field: " + ffrom + " from: " + source + " to field " + fto + " on " +target +" since API annotation is not specified.");
      // Skip fields which we do not need to fill
      if (direction  && apiano.direction()!=Direction.IN) return false;
      if (!direction && apiano.direction()==Direction.IN) return false;
      if (ft.getType().isAssignableFrom(ff.getType())) {
        ft.set(target, ff.get(source));
        return true;
      } else {
        // Find the right value transformer and transform the value.
        // ff.type -> ft.type
        TransfSig transfSig = tsig(ff.getType(), ft.getType());
        if (valueTransformers.containsKey(transfSig)) {
          ft.set(target, valueTransformers.get(transfSig).from(ff.get(source)));
          return true;
        }
        return false;
      }
    } catch (Throwable t) {
      throw new IllegalArgumentException("Cannot transfer field: " + ffrom + " from: " + System.identityHashCode(source) + " to field " + fto + " on " +System.identityHashCode(target), t);
    }
  }
  public static Field field(Class o, String name) throws SecurityException, NoSuchFieldException {
    while (o!=null && o != Object.class) {
      try {
        return o.getDeclaredField(name);
      } catch (NoSuchFieldException e) { o = o.getSuperclass(); }
    }
    // will throw exception always
    throw new NoSuchFieldException("There is no field called " +name);
  }
}
