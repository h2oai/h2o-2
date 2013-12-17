package water.api.dsl

trait DefaultEnv[K <: HexKey, T <: DFrame] {
  self: T_H2O_Env[K,T] with T_R_Env[T] =>
  
  def cars() = parse("../private/cars.csv");
  
  def source() = cars()(2 to 7)
  def response() = cars()("cylinders")
  
  //def drf(t : T): = drf(t, t(2), 2)
}