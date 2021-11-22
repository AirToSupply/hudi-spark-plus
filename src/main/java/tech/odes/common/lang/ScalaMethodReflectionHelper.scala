package tech.odes.common.lang

import scala.reflect.runtime.{universe => ru}

/**
 * 2021-11-09 Town(airtosupply@126.com)
 */
class ScalaMethodReflectionHelper(x: ru.InstanceMirror) {

  private var methodName: Option[ru.MethodSymbol] = None
  private var fieldName: Option[ru.TermSymbol] = None

  def method(name: String) = {
    methodName = Option(x.symbol.typeSignature.member(ru.TermName(name)).asMethod)
    this
  }

  def field(name: String) = {
    fieldName = Option(x.symbol.typeSignature.member(ru.TermName(name)).asTerm)
    this
  }

  def invoke(objs: Any*) = {

    if (methodName.isDefined) {
      x.reflectMethod(methodName.get.asMethod)(objs: _*)
    } else if (fieldName.isDefined) {
      val fieldMirror = x.reflectField(fieldName.get)
      if (objs.size > 0) {
        fieldMirror.set(objs.toSeq(0))
      }
      fieldMirror.get

    } else {
      throw new IllegalArgumentException("Can not invoke `invoke` without call method or field function")
    }


  }
}
