package water.api.dsl

import java.io._
import java.security._
import java.util.zip._
import scala.tools.nsc._
import scala.tools.nsc.interpreter._
import water.fvec.NFSFileVec
import water.fvec.Frame
import water.Boot
import water.H2O
import water.api.dsl.examples._

/** Custom H2O REPL.
 *  
 *  It reconfigures default REPL and  
 *  @see http://www.michaelpollmeier.com/create-your-custom-scala-repl/
 */
object ScAlH2ORepl {
  // Simple REPL launcher - expect to be executed from water.Boot 
  //  - hence it configures REPL classpath according the Boot
  def main(args: Array[String]): Unit = {
    // Launch H2O main
    H2O.main(args);
    // Launch REPL
    //launchRepl
    //XT.test2
    Examples.example2();
  }
  
  def launchRepl() = {
	  val repl = new H2OILoop
	  val settings = new Settings
	  settings.Xnojline.value = true // Use SimpleLine library by default
	  settings.Yreplsync.value = true
	  // FIXME we should provide CP via classloader via resource app.class.path 
	  // but currently we have a problem to create a correct compiler mirror since javassist modify the classloader 
	  settings.usejavacp.value = true
	  // setup the classloader of some H2O class
	  settings.embeddedDefaults[NFSFileVec]
	  //settings.embeddedDefaults[Boot]
      // Uncomment to DEBUG:
	  //settings.Yrepldebug.value = true
	  //settings.Ylogcp.value = true
	  //settings.verbose.value = true
	  
	  repl.process(settings)
  }
  
  /** H2O Repl Configuration */
  class H2OILoop extends ILoop {
    // override default shell prompt
    override def prompt = "h2o> "
    // implicitly import H2ODsl
    addThunk(intp.beQuietDuring {
      intp.addImports("water.api.dsl.H2ODsl._")
      intp.addImports("water.api.dsl._")
    })
    
    override def printWelcome() {
      echo("""
=== Welcome to the world of ScAlH2O ===
          
Type `help` or `example` to begin...
""")
    }
    
  }
}
