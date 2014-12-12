
Scala for H\ :sub:`2`\ O: Shalala
===================================

Overview
--------
Shalala is a Scala library providing access to H2O API via a dedicated DSL
and also a REPL integrated into H2O.

Currently, the library supports the following expressions abstracting H2O API.

*R-like commands*

::

  help
  ncol <frame>
  nrow <frame>
  head <frame>
  tail <frame>
  f(2)           - returns 2. column
  f("year")      - returns column "year"
  f(*,2)         - returns 2. column
  f(*, 2 to 5)   - returns 2., 3., 4., 5. columns
  f(*,2)+2       - scalar operation - 2.column + 2
  f(2)*3         - scalar operation - 2.column * 3
  f-1            - scalar operation - all columns - 1
  f < 10         - transform the frame into boolean frame respecting the condition


*H2O commands*

::

  keys              - shows all available keys i KV store
  parse("iris.csv") - parse given file and return a frame
  put("a.hex", f)   - put a frame into KV store
  get("b.hex")      - return a frame from KV store
  jobs              - shows a list of executed jobs
  shutdown          - shutdown H2O cloud

*M/R commands*

::


      f map (Add(3))   - call of map function of all columns in frame
                          - function is (Double=>Double) and has to extend Iced
      f map (Less(10)) - call of map function on all columns
                          - function is (Double=>Boolean) 


Build Scalala
-------------

To build Shalala `sbt` is required. You can get `sbt` from http://www.scala-sbt.org/release/docs/Getting-Started/Setup.

To compile Shalala please type:

::

  sbt compile


Launch REPL
-----------
Shalala provides an integrated Scala REPL exposing H2O DSL. 
You can start REPL via ``sbt``:

::

  sbt run

Launch Examples
---------------
Shalala provides a convenient way to run examples via ``sbt``:

::

  sbt runExamples


Key points of implementation
----------------------------
* Using primitive types specialization (to allow for generation code using primitive types)
* All objects passed around cloud has to inherits from ``water.Iced``

Examples
--------
::

  val f = parse("smalldata/cars.csv")

  f(2)           // number of cylinders

  f("year")      // year of production

  f(*, 0::2::7::Nil)  // year,number of cylinders and year

  f(7) map Sub(1000) // Subtract 1000 from year column

  f("cylinders") map (new BOp { 
      var sum:scala.Double = 0
      def apply(rhs:scala.Double) = { sum += rhs; rhs*rhs / sum; } 
    })


FAQs
----

* **How to generate Eclipse project and import it into Eclipse?**

  - Launch ``sbt`` shell

  - In ``sbt`` use the command ``eclipse`` to create Eclipse project files 
    ::
     
    > eclipse

  - In Eclipse use the ``Import Wizard`` to import the project into
    workspace


* **How to run REPL from Eclipse?**

  - Import *h2o-scala* project into Eclipse
  
  - Launch ``water.api.dsl.ShalalaRepl`` as a Scala application

* **How to generate Idea project and import it?**

  - Launch ``sbt``
  
  - In ``sbt`` use the command ``gen-idea`` to create Idea project files
    ::
  
    > gen-idea
  
  - In Idea open the project located in ``h2o-scala`` directory

""""