package water.api.dsl.examples

import water.api.dsl._

/**
 * Ideas:
 * GroupBy for H2O.
 * Histogram.
 * Matrix product.
 */
/*
val 

f( f('year)<1980, *) 

1) chci generatovat sumu v zavislosti na vice sloupcich
       |------ MAP SELECTOR ---------------|
f map ('year, 'cylinders) -> (scala.Double) {
  (year, cylinder) => { ...; ...; year*cylinder^2 }  // <- MAP implementation
} 

2) chci generovat nov frame v zavislosti na vsech sloupcich
 ~ ala filter
f map (*) -> (*) {
	(a:Array[Double], b:Array[NewChunk]) => { append to a new chunk }   
}
 
*/
/*
val frame := get("train.hex")

frame[,2 to 10]
  .map(Values[]) { ... do something ... } // // reprezentace jedne liny
  .reduce(op) { ... reduce ... }

frame[,2 to 10] returns a new frame with vector [2,3,...,10]

*/


