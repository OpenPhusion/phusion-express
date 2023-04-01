package cloud.phusion.express.component;

import cloud.phusion.Context;
import cloud.phusion.PhusionException;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JavaModuleManager {
    private static final String _position = JavaModuleManager.class.getName();

    private Map<String, URLClassLoader> jarClassLoaders;
    private String basePath;

    public JavaModuleManager() {
        this(null);
    }

    public JavaModuleManager(String basePath) {
        super();

        this.jarClassLoaders = new ConcurrentHashMap<String, URLClassLoader>();
        this.basePath = basePath;
    }

    public void loadJar(String moduleId, String[] paths, Context ctx) throws Exception {
        ctx.logInfo(_position, "Loading Java module", "paths="+(paths==null ? "null" : Arrays.toString(paths)));

        URL[] urls = new URL[paths.length];
        for (int i = 0; i < paths.length; i++) {
            String f = basePath==null ? paths[i] : basePath+paths[i];
            urls[i] = new File(f).toURI().toURL();
        }

        try {
            URLClassLoader loader = URLClassLoader.newInstance(urls);

            jarClassLoaders.put(moduleId, loader);
        } catch (Exception ex) {
            throw new PhusionException("JAR_OP", "Failed to load Java module", String.format("moduleId=%s, paths=%s",
                    moduleId, Arrays.toString(paths)), ctx, ex);
        }

        ctx.logInfo(_position, "Java module loaded");
    }

    public void unloadJar(String moduleId, Context ctx) throws Exception {
        URLClassLoader loader = jarClassLoaders.get(moduleId);
        if (loader == null) return;

        ctx.logInfo(_position, "Unloading Java module");

        loader.close();
        jarClassLoaders.remove(moduleId);

        ctx.logInfo(_position, "Java module unloaded");
    }

    public boolean doesJarExist(String moduleId) {
        return jarClassLoaders.containsKey(moduleId);
    }

    public Object createClassInstance(String moduleId, String className, Context ctx) throws Exception {
        if (! doesJarExist(moduleId))
            throw new PhusionException("JAR_NONE", "Failed to create instance", String.format("moduleId=%s, className=%s",
                    moduleId, className), ctx);

        Object result = null;

        try {
            URLClassLoader loader = jarClassLoaders.get(moduleId);
            result = loader.loadClass(className).newInstance();
        } catch (Exception ex) {
            throw new PhusionException("JAR_NONE", "Failed to create instance", String.format("moduleId=%s, className=%s",
                    moduleId, className), ctx, ex);
        }

        return result;
    }

}
