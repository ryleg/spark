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

package org.apache.spark.serializer

import java.net.URL
import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializationConstructor.TaskSerializers
import org.apache.spark.serializer.SerializationSchema.{ClassPathDescription, TaskPackingRequirements}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader, Utils}


trait ClassReflectInstHelp {

  def isDriver: Boolean

  def conf : SparkConf

  // Create an instance of the class with the given name, possibly initializing it with our conf
  def instantiateClass[T](className: String): T = {
    val cls = Utils.classForName(className)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, new java.lang.Boolean(isDriver))
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        try {
          cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
        } catch {
          case _: NoSuchMethodException =>
            cls.getConstructor().newInstance().asInstanceOf[T]
        }
    }
  }

  // Create an instance of the class named by the given SparkConf property, or defaultClassName
  // if the property is not set, possibly initializing it with our conf
  def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
    instantiateClass[T](conf.get(propertyName, defaultClassName))
  }

}


trait EnvLockedSerCons extends ClassReflectInstHelp {

  var sparkEnv : SparkEnv
  var sparkContext : SparkContext

  def conf: SparkConf = sparkEnv.conf

  import ClassLoaderUtil.constructClassLoader

  private val activeTaskRequirements =
    scala.collection.mutable.HashMap[
      ClassPathDescription,
      TaskPackingRequirements
      ]()

  /**
    * Given requirements to form a ClassLoader on an Executor for
    * loading tasks (i.e. the URL of the HTTP ClassServer for the REPL
    * and the corresponding classpath of jars / task specific jars.
    *
    * @param k : Paths of classes to load
    * @return Everything required to understand / decompose
    *          tasks
    */
  private def getOrElseUpdateTaskRequirements(
                                               k: ClassPathDescription
                                             ): TaskPackingRequirements = {
    activeTaskRequirements.getOrElseUpdate(k, {
      val jarURL = k.jarPath.split("\\:").map{q => new java.net.URL(q)}
      val cl = constructClassLoader(
        sparkEnv,
        jarURL,
        k.replPath
      )
      TaskPackingRequirements(
        cl,
        makeTaskSerializers(cl)
      )
    })
  }


}



object ClassLoaderUtil extends Logging {


  /**
    * If the REPL is in use, add another ClassLoader that will read
    * new classes defined by the REPL as the user types code
    */
  def addReplClassLoaderIfNeeded(
                                  sparkEnv: SparkEnv,
                                  parent: ClassLoader,
                                  dynamicClassUri: Option[String] = None,
                                  loadChildClassesFirst: Boolean = false
                                ): ClassLoader = {
    val classUri = dynamicClassUri.getOrElse(
      sparkEnv.conf.get("spark.repl.class.uri", null)
    )
    if (classUri != null) {
      try {
        logInfo("Using REPL class URI: " + classUri)
        val _userClassPathFirst: java.lang.Boolean = loadChildClassesFirst
        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
          classOf[String], classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(sparkEnv.conf, sparkEnv, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }


  /**
    * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
    * created by the interpreter to the search path
    */
  def createClassLoader(
                         jarURIs: Array[URL],
                         loadChildClassesFirst: Boolean = false
                       ): MutableURLClassLoader = {

    val currentLoader = Utils.getContextOrSparkClassLoader
    if (loadChildClassesFirst) {
      new ChildFirstURLClassLoader(jarURIs, currentLoader)
    } else {
      new MutableURLClassLoader(jarURIs, currentLoader)
    }
  }

  def constructClassLoader( sparkEnv: SparkEnv,
                            jarURIs: Array[URL],
                            classServer: String
                          ): ClassLoader = {
    val cl = createClassLoader(jarURIs)
    val rcl = addReplClassLoaderIfNeeded(sparkEnv, cl, Some(classServer))
    rcl
  }

  def baseCL: ClassLoader = Thread.currentThread().getContextClassLoader

}

object SerializationSchema {

  case class TaskPackingRequirements(
                                      classLoader: ClassLoader,
                                      taskSerializers: TaskSerializers
                                    )

  case class TaskSerializers(
                              resultSerializer: SerializerInstance,
                              closureSerializer: SerializerInstance
                            )

  case class ClassPathDescription(
                                   jarPath: String,
                                   replPath: String
                                 )

  def pathFromProperties(p: Properties): Option[ClassPathDescription] = {
    val jarPath = p.getProperty("spark.dynamic.jarPath")
    val replPath = p.getProperty("spark.dynamic.userReplPath")
    val keyCL = ClassPathDescription(jarPath, replPath)
    if (jarPath == null || replPath == null) None else Some{
      keyCL}
  }

}


object SerializationConstruction extends EnvLockedSerCons {

  var sparkEnv = null.asInstanceOf[SparkEnv]

  var sc: SparkContext = null.asInstanceOf[SparkContext]

  import SerializationSchema._

  def isDriver = sparkEnv.isDriver

  def mkSerializer(
                    confSpec: String = "spark.serializer"
                  ): Serializer = {
    instantiateClassFromConf[Serializer](
      confSpec,
      "org.apache.spark.serializer.JavaSerializer"
    ).setDefaultClassLoader(getClassLoader)
  }

  private def getClassPathProperties() = {
    if (sc != null && isDriver) { // This is a double check that
      // means we're the driver

    }
  }

  private def getClassLoader: ClassLoader = {
    sc.flatMap{
      s =>
        val path = pathFromProperties(s.getLocalProperties)
        path
          .map{getOrElseUpdateTaskRequirements}
          .map{_.classLoader}
    }.getOrElse(
      baseCL
    )
  }

  /**
    * Bind the serializers to the given ClassLoader upon
    * construction and return
    *
    * @param classLoader : From a given ClassServer URI / REPL /
    *                    user initiated class execution request.
    * @return Serializers required for task synchronization
    *         across machines bound to given ClassLoader
    */
  private def makeTaskSerializers(classLoader: ClassLoader): TaskSerializers = {
    TaskSerializers(serializer.setDefaultClassLoader(classLoader).newInstance(),
      closureSerializer.setDefaultClassLoader(classLoader).newInstance())
  }

  def closureSerializer: Serializer = {

  }

  def serializer: Serializer  = {

  }

}
