package water.api.rest;

/** It is like enum but provide typing */
public abstract class Version {
  static class V1 extends Version {}
  static class V2 extends Version {}
  static class Bloody extends Version {}
  public static final V1 v1 = new V1();
  public static final V2 v2 = new V2();
  public static final Bloody bloody = new Bloody();
}
