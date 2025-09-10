/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.reflection;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.xtable.exception.ConfigurationException;

/** Creates a instance of class from the class name and provided constructor arguments. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ReflectionUtils {

  public static <T> T createInstanceOfClass(String className, Object... constructorArgs) {
    Class<T> clazz;
    try {
      clazz = (Class<T>) ReflectionUtils.class.getClassLoader().loadClass(className);
    } catch (ClassNotFoundException ex) {
      throw new ConfigurationException("Class not found: " + className, ex);
    }
    try {
      if (constructorArgs.length == 0) {
        return clazz.newInstance();
      }
      for (Constructor<?> constructor : clazz.getConstructors()) {
        Class<?>[] parameterTypes = constructor.getParameterTypes();
        if (parameterTypes.length == constructorArgs.length) {
          boolean matches = true;
          for (int i = 0; i < parameterTypes.length; i++) {
            if (!parameterTypes[i].isAssignableFrom(constructorArgs[i].getClass())) {
              matches = false;
              break;
            }
          }
          if (matches) {
            return (T) constructor.newInstance(constructorArgs);
          }
        }
      }
      throw new NoSuchMethodException(
          "Could not find a suitable constructor for class: " + className);

    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException e) {
      throw new ConfigurationException("Unable to load class: " + className, e);
    }
  }

  public static <T> T createInstanceOfClassFromStaticMethod(
      String className, String methodName, Class<?>[] argClasses, Object[] args) {
    try {
      // try loading the class; throw error if not found
      Class<T> clazz = (Class<T>) ReflectionUtils.class.getClassLoader().loadClass(className);

      // Retrieve and make the specified method accessible
      Method method = clazz.getDeclaredMethod(methodName, argClasses);
      method.setAccessible(true);

      // Invoke the method if it's static; throw an error otherwise
      if (Modifier.isStatic(method.getModifiers())) {
        return (T) method.invoke(null, args);
      } else {
        throw new IllegalArgumentException("The specified method is not static: " + methodName);
      }
    } catch (ClassNotFoundException ex) {
      throw new ConfigurationException("Unable to load class: " + className, ex);
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | IllegalArgumentException ex) {
      throw new ConfigurationException(
          String.format("Failed to invoke method '%s' in class '%s'", methodName, className), ex);
    }
  }

  public static <T> T createInstanceOfClassFromStaticMethod(String className, String methodName) {
    return createInstanceOfClassFromStaticMethod(className, methodName, new Class<?>[] {}, null);
  }

  private static boolean hasConstructor(Class<?> clazz, Class<?>... constructorArgTypes) {
    try {
      clazz.getConstructor(constructorArgTypes);
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }
}
