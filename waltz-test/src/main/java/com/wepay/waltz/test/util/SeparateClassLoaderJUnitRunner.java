package com.wepay.waltz.test.util;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class SeparateClassLoaderJUnitRunner extends Runner {

    private final Runner inner;

    public SeparateClassLoaderJUnitRunner(Class<?> clazz) throws InitializationError {
        try {
            ClassLoader classLoader = AccessController.doPrivileged(
                (PrivilegedAction<ClassLoader>) SeparateClassLoader::new
            );
            this.inner = getRunner(classLoader.loadClass(clazz.getName()));

        } catch (InitializationError ex) {
            throw ex;

        } catch (Throwable ex) {
            throw new InitializationError(ex);
        }
    }

    @Override
    public Description getDescription() {
        return inner.getDescription();
    }

    @Override
    public void run(RunNotifier notifier) {
        inner.run(notifier);
    }

    private Runner getRunner(Class<?> clazz) throws InitializationError {
        return new BlockJUnit4ClassRunner(clazz);
    }

    private static class SeparateClassLoader extends URLClassLoader {

        SeparateClassLoader() {
            super(((URLClassLoader) getSystemClassLoader()).getURLs());
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            if (name.startsWith("com.wepay.")) {
                return super.findClass(name);

            } else {
                return super.loadClass(name);
            }
        }
    }

}
