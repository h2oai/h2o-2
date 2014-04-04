package water.api.dsl

import scala.tools.nsc._
import scala.tools.nsc.interpreter._
import water.fvec.NFSFileVec
import water.Boot
import water.H2O

/** Custom H2O REPL.
 *  
 *  It reconfigures default REPL and  
 *  @see http://www.michaelpollmeier.com/create-your-custom-scala-repl/
 */
object ShalalaRepl {
  // Simple REPL launcher - expect to be executed from water.Boot 
  //  - hence it configures REPL classpath according the Boot
  def main(args: Array[String]): Unit = {
    // Launch Boot and and then H2O and ShalalaRepl 
    Boot.main(classOf[ShalalaRepl], args)
  }
  
  def userMain(args: Array[String]):Unit = {
    H2O.main(args)
	ShalalaRepl.launchRepl
  }

  def launchRepl() = {
	  val repl = new H2OILoop
	  val settings = new Settings
	  //settings.Xnojline.value = true // Use SimpleLine library by default
	  //settings.Yreplsync.value = true
	  // FIXME we should provide CP via classloader via resource app.class.path 
	  settings.usejavacp.value = true
	  // setup the classloader of some H2O class
	  settings.embeddedDefaults[NFSFileVec]
	  //settings.embeddedDefaults[Boot] // serve as application loader
      // Uncomment to configure DEBUG
	  //debug(settings)
	  
	  repl.process(settings)
  }
  
  def debug(settings: Settings) = {
    //settings.Yrepldebug.value = true
    settings.Ylogcp.value = true
	settings.verbose.value = true
  }
  
  /** H2O Repl Configuration */
  class H2OILoop extends ILoop {
    // override default shell prompt
    override def prompt = "h2o> "
    // implicitly import H2ODsl
    addThunk(intp.beQuietDuring {
      intp.addImports("water.api.dsl.H2ODsl._")
      intp.addImports("water.api.dsl._")
      intp.addImports("water.api.Request.API")
      intp.addImports("hex.deeplearning.DeepLearning")
      intp.addImports("hex.deeplearning.DeepLearning.ClassSamplingMethod")
    })
    
    override def printWelcome() {
      echo("""
=== Welcome to the world of Shalala ===
          
Type `help` or `example` to begin...
          
Your are now in """ + System.getProperty("user.dir") + """

Enjoy!
"""
)
    }
  }
}

// Companion class
class ShalalaRepl {
}
