/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.oap.vectorized;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/** Helper class for JNI related operations. */
public class JniUtils {
  private static final String LIBRARY_NAME = "spark_columnar_jni";
  private static boolean isLoaded = false;
  private static boolean isCodegenDependencyLoaded = false;
  private static List<String> codegenJarsLoadedCache = new ArrayList<>();
  private static volatile JniUtils INSTANCE;
  private String tmp_dir;

  public static JniUtils getInstance() throws IOException {
    String tmp_dir = System.getProperty("java.io.tmpdir");
    return getInstance(tmp_dir);
  }

  public static JniUtils getInstance(String tmp_dir) throws IOException {
    if (INSTANCE == null) {
      synchronized (JniUtils.class) {
        if (INSTANCE == null) {
          try {
            INSTANCE = new JniUtils(tmp_dir);
          } catch (IllegalAccessException ex) {
            throw new IOException("IllegalAccess");
          }
        }
      }
    }
    return INSTANCE;
  }

  private JniUtils(String tmp_dir) throws IOException, IllegalAccessException, IllegalStateException {
    if (!isLoaded) {
      try {
        loadLibraryFromJar(tmp_dir);
      } catch (IOException ex) {
        System.loadLibrary(LIBRARY_NAME);
      }
      isLoaded = true;
    }
  }

  public void setTempDir(String _tmp_dir) throws IOException, IllegalAccessException {
    if (isCodegenDependencyLoaded == false) {
      tmp_dir = _tmp_dir;
      loadIncludeFromJar(tmp_dir);
      loadLibFromJar(tmp_dir);
      isCodegenDependencyLoaded = true;
    }
  }

  public void setJars(List<String> list_jars) throws IOException, IllegalAccessException {
    for (String jar : list_jars) {
      if (!codegenJarsLoadedCache.contains(jar)) {
        loadLibraryFromJar(jar, tmp_dir);
        codegenJarsLoadedCache.add(jar);
      }
    }
  }

  static void loadLibraryFromJar(String tmp_dir) throws IOException, IllegalAccessException {
    synchronized (JniUtils.class) {
      if (tmp_dir == null) {
        tmp_dir = System.getProperty("java.io.tmpdir");
        System.out.println("loadLibraryFromJar " + tmp_dir);
      }
      final String libraryToLoad = System.mapLibraryName(LIBRARY_NAME);
      final File libraryFile = moveFileFromJarToTemp(tmp_dir, libraryToLoad);
      System.load(libraryFile.getAbsolutePath());
    }
  }

  private static void loadLibFromJar(String tmp_dir) throws IOException, IllegalAccessException {
    synchronized (JniUtils.class) {
      if (tmp_dir == null) {
        tmp_dir = System.getProperty("java.io.tmpdir");
        System.out.println("loadLibFromJar " + tmp_dir);
      }
      final String folderToLoad = "lib";
      final URLConnection urlConnection = JniUtils.class.getClassLoader().getResource("lib").openConnection();
      if (urlConnection instanceof JarURLConnection) {
        final JarFile jarFile = ((JarURLConnection) urlConnection).getJarFile();
        copyResourcesToDirectory(jarFile, folderToLoad, tmp_dir + "/");
      } else {
        throw new IOException(urlConnection.toString() + " is not JarUrlConnection");
      }
    }
  }

  private static void loadLibraryFromJar(String source_jar, String tmp_dir) throws IOException, IllegalAccessException {
    synchronized (JniUtils.class) {
      if (tmp_dir == null) {
        tmp_dir = System.getProperty("java.io.tmpdir");
        System.out.println("loadLibraryFromJar " + tmp_dir);
      }
      final String folderToLoad = "";
      URL url = new URL("jar:file:" + source_jar + "!/");
      final URLConnection urlConnection = (JarURLConnection) url.openConnection();
      File tmp_dir_handler = new File(tmp_dir + "/tmp");
      if (!tmp_dir_handler.exists()) {
        tmp_dir_handler.mkdirs();
      }

      if (urlConnection instanceof JarURLConnection) {
        final JarFile jarFile = ((JarURLConnection) urlConnection).getJarFile();
        copyResourcesToDirectory(jarFile, folderToLoad, tmp_dir + "/tmp/");
      } else {
        throw new IOException(urlConnection.toString() + " is not JarUrlConnection");
      }
      /*
       * System.out.println("Current content under " + tmp_dir + "/tmp/");
       * Files.list(new File(tmp_dir + "/tmp/").toPath()).forEach(path -> {
       * System.out.println(path); });
       */
    }
  }

  private static void loadIncludeFromJar(String tmp_dir) throws IOException, IllegalAccessException {
    synchronized (JniUtils.class) {
      if (tmp_dir == null) {
        tmp_dir = System.getProperty("java.io.tmpdir");
        System.out.println("loadIncludeFromJar " + tmp_dir);
      }
      final String folderToLoad = "include";
      final URLConnection urlConnection = JniUtils.class.getClassLoader().getResource("include").openConnection();
      if (urlConnection instanceof JarURLConnection) {
        final JarFile jarFile = ((JarURLConnection) urlConnection).getJarFile();
        copyResourcesToDirectory(jarFile, folderToLoad, tmp_dir + "/" + "nativesql_include");
      } else {
        throw new IOException(urlConnection.toString() + " is not JarUrlConnection");
      }
    }
  }

  private static File moveFileFromJarToTemp(String tmpDir, String libraryToLoad) throws IOException {
    // final File temp = File.createTempFile(tmpDir, libraryToLoad);
    final File temp = new File(tmpDir + "/" + libraryToLoad);
    try (final InputStream is = JniUtils.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new FileNotFoundException(libraryToLoad);
      } else {
        try {
          Files.copy(is, temp.toPath());
        } catch (Exception e) {
        }
      }
    }
    return temp;
  }

  /**
   * Copies a directory from a jar file to an external directory.
   */
  public static void copyResourcesToDirectory(JarFile fromJar, String jarDir, String destDir) throws IOException {
    for (Enumeration<JarEntry> entries = fromJar.entries(); entries.hasMoreElements();) {
      JarEntry entry = entries.nextElement();
      if (((jarDir == "" && !entry.getName().contains("META-INF")) || (entry.getName().startsWith(jarDir + "/")))
          && !entry.isDirectory()) {
        int rm_length = jarDir.length() == 0 ? 0 : jarDir.length() + 1;
        File dest = new File(destDir + "/" + entry.getName().substring(rm_length));
        File parent = dest.getParentFile();
        if (parent != null) {
          parent.mkdirs();
        }

        FileOutputStream out = new FileOutputStream(dest);
        InputStream in = fromJar.getInputStream(entry);

        try {
          byte[] buffer = new byte[8 * 1024];

          int s = 0;
          while ((s = in.read(buffer)) > 0) {
            out.write(buffer, 0, s);
          }
        } catch (IOException e) {
          throw new IOException("Could not copy asset from jar file", e);
        } finally {
          try {
            in.close();
          } catch (IOException ignored) {
          }
          try {
            out.close();
          } catch (IOException ignored) {
          }
        }
      }
    }
  }
}
