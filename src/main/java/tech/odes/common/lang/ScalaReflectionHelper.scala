package tech.odes.common.lang

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
  * 2021-11-09 Town(airtosupply@126.com)
  */
class ScalaReflectionHelper {}

object ScalaReflectionHelper {
  private def mirror = {
    ru.runtimeMirror(getClass.getClassLoader)
  }

  def fromInstance[T: ClassTag](obj: T) = {
    val x = mirror.reflect[T](obj)
    new ScalaMethodReflectionHelper(x)
  }

  def fromObject[T: ru.TypeTag]() = {
    new ScalaModuleReflectionHelper(ru.typeOf[T].typeSymbol.companion.asModule)
  }

  def fromObjectStr(str: String) = {
    val module = mirror.staticModule(str)
    new ScalaModuleReflectionHelper(module)
  }

  //def getClass[T: ru.TypeTag](obj: T) = ru.typeTag[T].tpe.typeSymbol.asClass
}
