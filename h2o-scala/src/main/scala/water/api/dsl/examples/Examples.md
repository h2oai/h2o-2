# Examples

Motivate by R and Scalding syntax.

## Select and filtering columns
f( f('year)<1980, *) 

## MAP operator
Expect the predefined number of IN vectors, and OUT vectors/values

## Generate a new vector(s) depending on selected vectors
       
```scala
f map ('year, 'cylinders) -> ('new_vec) {
  (year:Double, cylinder:Double) => { ...; ...; year*cylinder^2 }  // <- MAP implementation
}
``` 

## Generate a new frame based on original frame, a new frame contains 
the same number of vectors as original frame, but can contain less rows
(perhaps map should always serves the same number of row, and then we can have a filter)
```scala
f map ( (*) -> (*) ) {
	(row:Array[Double], oc:Array[NewChunk]) => { append to a new chunk }   
}
```
 
### Generate sums of all vectors
IN: vector selector
OUT: 
```scala
f map ( (*) -> (scala.Double) )
             { (row:Array[Double], out:Array[Double]) => { /* fill out array */ } } 
          ~> { (v1:Array[Double], v2:Array[Double]) =>  {  /* reduce out arrays */ }
```


## ?Fold?
To produce scalar values (not vector)


# Limitations
  - everything passed over network has to have Freezable iface or inherits from Iced
  - we deal only with primitive types, Scala API should generate specialization for int/long/double 
  primitive types (@specialized annotation)
  
# Questions
  - using macros?
  
  