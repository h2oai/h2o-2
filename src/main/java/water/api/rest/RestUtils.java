package water.api.rest;

import java.lang.reflect.Field;
import java.util.*;

import water.Iced;
import water.api.Direction;
import water.api.Request.API;
import water.api.rest.REST.TransfSig;
import water.api.rest.REST.ValueTransf;
import water.api.rest.schemas.ApiSchema;

public class RestUtils {
  static final Class [] p(Class a, Class b) { return new Class[] { a, b}; }
  static final String[] p(String a, String b) { return new String[] { a, b }; }
  static final String[][] map(String[] ...s)  { return s; }
  public static TransfSig tsig(Class a, Class b) { return new TransfSig(a, b); }

  public static <A extends ApiSchema, I extends Iced> boolean fillAPIField(I source, String ffrom, A target, String fto, Map<TransfSig, ValueTransf> valueTransformers) {
    return fillField(source, ffrom, target, fto, valueTransformers, false);
  }
  public static <A extends ApiSchema, I extends Iced> boolean fillImplField(A source, String ffrom, I target, String fto, Map<TransfSig, ValueTransf> valueTransformers) {
    return fillField(source, ffrom, target, fto, valueTransformers, true);
  }
  public static boolean fillField(Object source, String ffrom, Object target, String fto, Map<TransfSig, ValueTransf> valueTransformers, boolean direction /* true: source == API, false: target == API */) {
    try {
      Field ff = field(source.getClass(),ffrom); ff.setAccessible(true);
      Field ft = field(target.getClass(),fto); ft.setAccessible(true);
      return fillField(source, ff, target, ft, valueTransformers, direction);
    } catch (NoSuchFieldException t) {
      throw new IllegalArgumentException("Cannot transfer field: " + ffrom + " from: " + System.identityHashCode(source) + " to field " + fto + " on " +System.identityHashCode(target), t);
    }
  }
  public static boolean fillField(Object source, Field ffrom, Object target, Field fto, Map<TransfSig, ValueTransf> valueTransformers, boolean direction /* true: source == API, false: target == API */) {
    try {
      API apiano = direction ? ffrom.getAnnotation(API.class) : fto.getAnnotation(API.class);
      if (apiano==null) throw new IllegalArgumentException("Cannot transfer field: " + ffrom + " from: " + source + " to field " + fto + " on " +target +" since API annotation is not specified.");
      // Skip fields which we do not need to fill
      if (direction  && apiano.direction()!=Direction.IN) return false;
      if (!direction && apiano.direction()==Direction.IN) return false;
      if (fto.getType().isAssignableFrom(ffrom.getType())) {
        fto.set(target, ffrom.get(source));
        return true;
      } else {
        // Find the right value transformer and transform the value.
        // ff.type -> ft.type
        TransfSig transfSig = tsig(ffrom.getType(), fto.getType());
        if (valueTransformers.containsKey(transfSig)) {
          fto.set(target, valueTransformers.get(transfSig).from(ffrom.get(source)));
          return true;
        }
        return false;
      }
    } catch (Throwable t) {
      throw new IllegalArgumentException("Cannot transfer field: " + ffrom + " from: " + System.identityHashCode(source) + " to field " + fto + " on " +System.identityHashCode(target), t);
    }
  }
  public static Field field(Class o, String name) throws NoSuchFieldException {
    while (o!=null && o != Object.class) {
      try {
        return o.getDeclaredField(name);
      } catch (NoSuchFieldException e) { o = o.getSuperclass(); }
    }
    // will throw exception always
    throw new NoSuchFieldException("There is no field called " +name);
  }
  public static Field safeField(Class o, String name) {
    while (o!=null && o != Object.class) {
      try {
        return o.getDeclaredField(name);
      } catch (NoSuchFieldException e) { o = o.getSuperclass(); }
    }
    return null;
  }

  public static <A extends ApiSchema> Field[] listAPIFields(A api, boolean in) {
    Field[] fields = api.getClass().getDeclaredFields(); // FIXME: should traverse also parents !
    List<Field> fs = new ArrayList<Field>(fields.length);
    for (Field f : fields) {
      f.setAccessible(true);
      API apiano = f.getAnnotation(API.class);
      if (apiano!=null) {
        boolean add = false;
        switch( apiano.direction() ) {
          case INOUT: add = true; break;
          case IN:    add = in; break;
          case OUT:   add = !in; break;
        }
        if (add) fs.add(f);
      }
    }
    return fs.toArray(new Field[fs.size()]);
  }

  public static <A extends ApiSchema, I extends Iced> void fillAPIFields(I source, A target, Map<TransfSig, ValueTransf> valueTransformers) {
    Field[] fields = RestUtils.listAPIFields(target, false);
    for (Field fto : fields) {
      Field ffrom = safeField(source.getClass(), fto.getName());
      if (ffrom!=null) fillField(source, ffrom, target, fto, valueTransformers, false);
    }
  }
  public static <A extends ApiSchema, I extends Iced> void fillImplFields(A source, I target, Map<TransfSig, ValueTransf> valueTransformers) {
    Field[] fields = RestUtils.listAPIFields(source, true);
    for (Field ffrom : fields) {
      Field fto = safeField(target.getClass(), ffrom.getName());
      if (ffrom!=null) fillField(source, ffrom, target, fto, valueTransformers, true);
    }
  }
}
