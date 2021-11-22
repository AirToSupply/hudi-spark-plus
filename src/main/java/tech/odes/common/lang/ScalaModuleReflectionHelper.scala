package tech.odes.common.lang

import scala.reflect.runtime.{universe => ru}

/**
 * 2021-11-09 Town(airtosupply@126.com)
 */
class ScalaModuleReflectionHelper(x: ru.ModuleSymbol) {

  private var methodName: Option[ru.MethodSymbol] = None
  private var fieldName: Option[ru.TermSymbol] = None

  def method(name: String) = {
    methodName = Option(x.typeSignature.member(ru.TermName(name)).asMethod)
    this
  }

  def field(name: String) = {
    fieldName = Option(x.typeSignature.member(ru.TermName(name)).asTerm)
    this
  }

  private def mirror = {
    ru.runtimeMirror(getClass.getClassLoader)
  }

  def invoke(objs: Any*) = {

    if (methodName.isDefined) {
      val instance = mirror.reflectModule(x).instance
      mirror.reflect(instance).reflectMethod(methodName.get.asMethod)(objs)
    } else if (fieldName.isDefined) {
      val instance = mirror.reflectModule(x).instance
      val fieldMirror = mirror.reflect(instance).reflectField(fieldName.get)
      if (objs.size > 0) {
        fieldMirror.set(objs.toSeq(0))
      }
      fieldMirror.get

    } else {
      throw new IllegalArgumentException("Can not invoke `invoke` without call method or field function")
    }

  }
}
