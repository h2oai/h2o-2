package water.deploy;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.jar.*;

import javassist.*;
import water.*;
import water.api.DocGen;
import water.util.Utils;

public class LaunchJar extends Request2 {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;

  @API(help = "Jars keys", required = true, filter = Default.class)
  public String jars;

  @API(help = "Class to instantiate and launch", required = true, filter = Default.class)
  public String job_class;

  @Override protected Response serve() {
    final Job job;
    try {
      // Move jars from KV store to tmp files
      ClassPool pool = new ClassPool(true);
      ArrayList<JarEntry> entries = new ArrayList<JarEntry>();
      String[] splits = jars.split(",");
      for( int i = 0; i < splits.length; i++ ) {
        Key key = Key.make(splits[i]);
        throw H2O.unimpl();
        //ValueArray va = UKV.get(key);
        //File file = File.createTempFile("h2o", ".jar");
        //Utils.writeFileAndClose(file, va.openStream());
        //DKV.remove(key);
        //pool.appendClassPath(file.getPath());
        //
        //JarFile jar = new JarFile(file);
        //Enumeration e = jar.entries();
        //while( e.hasMoreElements() ) {
        //  JarEntry entry = (JarEntry) e.nextElement();
        //  entries.add(entry);
        //}
        //jar.close();
      }

      // Append UID to class names so allow multiple invocations
      String uid = Key.rand();
      ClassMap renames = new ClassMap();
      for( JarEntry entry : entries ) {
        if( entry.getName().endsWith(".class") ) {
          String n = Utils.className(entry.getName());
          String u;
          int index = n.indexOf("$");
          if( index < 0 )
            index = n.length();
          u = n.substring(0, index) + uid + n.substring(index);
          renames.put(n, u);
        }
      }
      ArrayList<CtClass> updated = new ArrayList();
      for( Entry<String, String> entry : ((Map<String, String>) renames).entrySet() ) {
        CtClass c = pool.get(entry.getKey().replace('/', '.'));
        c.replaceClassName(renames);
        updated.add(c);
      }

      // Create jar file and register it on each node
      HashSet<String> packages = new HashSet();
      ByteArrayOutputStream mem = new ByteArrayOutputStream();
      JarOutputStream jar = new JarOutputStream(mem);
      DataOutputStream bc = new DataOutputStream(jar);
      for( CtClass c : updated ) {
        jar.putNextEntry(new JarEntry(c.getName().replace('.', '/') + ".class"));
        c.toBytecode(bc);
        bc.flush();
        String p = c.getPackageName();
        if( p == null )
          throw new IllegalArgumentException("Package is null for class " + c);
        packages.add(p);
      }
      jar.close();
      weavePackages(packages.toArray(new String[0]));
      AddJar task = new AddJar();
      task._data = mem.toByteArray();
      task.invokeOnAllNodes();

      // Start job
      Class c = Class.forName(job_class + uid);
      job = (Job) c.newInstance();
      job.fork();
    } catch( Exception ex ) {
      throw new RuntimeException(ex);
    }
    return Response.done(this);
  }

  public static void weavePackages(String... names) {
    WeavePackages task = new WeavePackages();
    task._names = names;
    task.invokeOnAllNodes();
  }

  static class WeavePackages extends DRemoteTask {
    String[] _names;

    @Override public void lcompute() {
      for( String name : _names )
        Boot.weavePackage(name);
      tryComplete();
    }

    @Override public void reduce(DRemoteTask drt) {
    }
  }

  static class AddJar extends DRemoteTask {
    byte[] _data;

    @Override public void lcompute() {
      try {
        File file = File.createTempFile("h2o", ".jar");
        Utils.writeFileAndClose(file, new ByteArrayInputStream(_data));
        Boot._init.addExternalJars(file);
        tryComplete();
      } catch( Exception ex ) {
        throw new RuntimeException(ex);
      }
    }

    @Override public void reduce(DRemoteTask drt) {
    }
  }
}
