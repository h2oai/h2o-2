package water.util;

import java.io.File;
import java.lang.reflect.Method;
import java.net.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import water.Log;
import water.TestUtil;

/**
 * Executes code in a class loader to emulate a separate process.
 */
public class SeparateCL extends Thread implements Separate {
  private final URL[]          _classpath;
  private final String         _mainClassName;
  private final WritableFuture _future = new WritableFuture();
  private ClassLoader          _initialClassLoader, _classLoader;
  private String[]             _args   = new String[0];
  private Class<?>             _mainClass;

  public SeparateCL(String mainClass) {
    _mainClassName = mainClass;
    _classpath = getClassPath();
    setDaemon(true);
  }

  static URL[] getClassPath() {
    String[] classpath = System.getProperty("java.class.path").split(System.getProperty("path.separator"));

    try {
      final List<URL> list = new ArrayList<URL>();

      if( classpath != null )
        for( final String element : classpath ) {
          list.addAll(getDirectoryClassPath(element));
          list.add(new File(element).toURI().toURL());
        }

      return list.toArray(new URL[list.size()]);
    } catch( Exception e ) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void waitForEnd() {
    try {
      join();
    } catch( InterruptedException e ) {
      Log.write(e);
    }
  }

  static void waitForProgress(AtomicInteger progress, int value) {
    while( progress.get() < value ) {
      try {
        Thread.sleep(1);
      } catch( InterruptedException e ) {
      }
    }
  }

  public String[] getArgs() {
    return _args;
  }

  public void setArgs(String... value) {
    _args = value;
  }

  Object getResult() {
    try {
      return _future.get();
    } catch( Exception e ) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    run(true);
  }

  public void run(boolean close) {
    _initialClassLoader = Thread.currentThread().getContextClassLoader();
    _classLoader = new URLClassLoader(_classpath, null);
    Thread.currentThread().setContextClassLoader(_classLoader);
    Exception ex = null;

    try {
      _mainClass = _classLoader.loadClass(_mainClassName);
      Method method = _mainClass.getMethod("main", new Class[] { String[].class });
      _future.setResult(method.invoke(null, (Object[]) _args));
    } catch( Exception e ) {
      Log.write(e);
      ex = e;
    }

    Thread.currentThread().setContextClassLoader(_initialClassLoader);

    if( ex != null && !(ex.getCause() instanceof InterruptedException) )
      throw new RuntimeException(ex.getCause());

    if( close )
      close();
  }

  public void close() {
    invoke(SeparateCL.class.getName(), "close_", new Class[0]);
  }

  public static void close_() {
    TestUtil.checkLeakedKeys();
  }

  Object invoke(String methodName) {
    return invoke(methodName, new Class[0]);
  }

  Object invoke(String methodName, Class[] argsTypes, Object... args) {
    return invoke(_mainClassName, methodName, argsTypes, args);
  }

  Object invoke(String className, String methodName) {
    return invoke(className, methodName, new Class[0]);
  }

  Object invoke(String className, String methodName, Class[] argsTypes, Object... args) {
    assert Thread.currentThread().getContextClassLoader() == _initialClassLoader;
    Thread.currentThread().setContextClassLoader(_classLoader);

    try {
      Class<?> c = _classLoader.loadClass(className);
      Method method = c.getMethod(methodName, argsTypes);
      method.setAccessible(true);
      return method.invoke(null, args);
    } catch( Exception ex ) {
      throw new RuntimeException(ex);
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
      throw new RuntimeException(e);
    }
  }

  private static final class WritableFuture extends FutureTask<Object> {
    public WritableFuture() {
      super(new Callable<Object>() {

        public Object call() throws Exception {
          return null;
        }
      });
    }

    public void setResult(Object value) {
      super.set(value);
    }
  }
}
