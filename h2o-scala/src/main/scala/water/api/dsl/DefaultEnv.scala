package water.api.dsl

trait DefaultEnv[K <: HexKey, T <: DFrame] {
  self: T_H2O_Env[K,T] with T_R_Env[T] =>
  
  def cars() = parse("../private/cars.csv");
  
  //def drf(t : T): = drf(t, t(2), 2)
}