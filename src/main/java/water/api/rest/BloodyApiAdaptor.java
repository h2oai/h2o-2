package water.api.rest;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import water.Iced;
import water.api.Request.*;
import water.api.rest.REST.AbstractApiAdaptor;
import water.api.rest.REST.RestCall;
import water.api.rest.Version.Bloody;

/**
 *
 *
 * @param <I> implementation type
 * @param <A> api type
 */
public abstract class BloodyApiAdaptor<I extends Iced, A extends RestCall<Bloody>> extends AbstractApiAdaptor<I, A, Version.Bloody> {
  @Override public A fillApi(I impl, A api) {
    Field[] fields = listAPIFields(api, false);
    for (Field f : fields) {
      if (!fillField(api, f, impl)) System.err.println("fillApi: Field " +f+" was not filled!");
    }
    return api;
  }
  @Override public I fillImpl(A api, I impl) {
    Field[] fields = listAPIFields(api, true);
    for (Field f : fields) {
      if (!fillField(impl, f, api)) System.err.println("fillImpl: Field " +f+" was not filled!");
    }
    return impl;
  }

  public Field[] listAPIFields(A api, boolean in) {
    Field[] fields = api.getClass().getFields();
    List<Field> fs = new ArrayList<Field>(fields.length);
    for (Field field : fields) {
      API apiano = field.getAnnotation(API.class);
      if (apiano!=null) {
        boolean add = false;
        switch( apiano.direction() ) {
          case INOUT: add = true; break;
          case IN:    add = in; break;
          case OUT:   add = !in; break;
        }
        if (add) fs.add(field);
      }
    }
    return fs.toArray(new Field[fs.size()]);
  }

  public boolean fillField(Object target, Field f, Object source) {
    Field targetField;
    try {
      targetField = target.getClass().getField(f.getName());
    } catch( Throwable e ) {
      return false;
    }
    if (targetField==null) return false;
    try {
      targetField.set(target, f.get(source));
    } catch( Throwable e) {
      return false;
    }
    return true;
  }

  @Override public water.api.rest.Version.Bloody getVersion() { return Version.bloody; };
}
