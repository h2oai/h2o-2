package water.deploy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.*;
import java.util.*;

import water.Boot;
import water.util.Log;

/**
 * Creates a node in-process using a separate class loader.
 */
public class NodeCL extends Thread implements Node {
  private final URL[] _classpath;
  private final String[] _args;
  private final Class _main;
  private ClassLoader _initialClassLoader, _classLoader;

  public NodeCL(Class main, String[] args) {
    super("NodeCL");
    _args = args;
    _main = main;
    _classpath = getClassPath();
    _initialClassLoader = Thread.currentThread().getContextClassLoader();
    _classLoader = new URLClassLoader(_classpath, null);
  }

  @Override public void inheritIO() {
    // TODO add -id to PID?
    // invoke(className, methodName, args)
  }

  @Override public void persistIO(String outFile, String errFile) throws IOException {
    // TODO
    // invoke(className, methodName, args)
  }

  @Override public void kill() {
    // TODO
    // invoke(className, methodName, args)
  }

  static URL[] getClassPath() {
    String[] classpath = System.getProperty("java.class.path").split(File.pathSeparator);
    try {
      final List<URL> list = new ArrayList<URL>();
      if( classpath != null ) {
        for( final String element : classpath ) {
          list.addAll(getDirectoryClassPath(element));
          list.add(new File(element).toURI().toURL());
        }
      }
      return list.toArray(new URL[list.size()]);
    } catch( Exception e ) {
      throw Log.errRTExcept(e);
    }
  }

  @Override public int waitFor() {
    try {
      join();
      return 0;
    } catch( InterruptedException e ) {
      throw Log.errRTExcept(e);
    }
  }

  @Override public void run() {
    assert Thread.currentThread().getContextClassLoader() == _initialClassLoader;
    Thread.currentThread().setContextClassLoader(_classLoader);

    try {
      Class<?> c = _classLoader.loadClass(Context.class.getName());
      Method method = c.getMethod("run", String.class, String[].class);
      method.setAccessible(true);
      method.invoke(null, _main.getName(), _args);
    } catch( Exception e ) {
      throw Log.errRTExcept(e);
    } finally {
      Thread.currentThread().setContextClassLoader(_initialClassLoader);
    }
  }

  private static List<URL> getDirectoryClassPath(String aDir) {
    try {
      final List<URL> list = new LinkedList<URL>();
      final File dir = new File(aDir);
      final URL directoryURL = dir.toURI().toURL();
      final String[] children = dir.list();

      if( children != null ) {
        for( final String element : children ) {
          if( element.endsWith(".jar") ) {
            final URL url = new URL(directoryURL, URLEncoder.encode(element, "UTF-8"));
            list.add(url);
          }
        }
      }
      return list;
    } catch( Exception e ) {
      throw Log.errRTExcept(e);
    }
  }

  static class Context {
    public static void run(String main, String[] args) throws Exception {
      // Boot takes SystemClassLoader as parent, override with ours
      Field parent = ClassLoader.class.getDeclaredField("parent");
      parent.setAccessible(true);
      parent.set(Boot._init, Thread.currentThread().getContextClassLoader());
      Boot.main(Class.forName(main), args);
    }
  }
}
