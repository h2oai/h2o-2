package h2o.waterWorks
//groovy
import groovy.util.ConfigObject;
import groovy.util.ConfigSlurper
import groovy.lang.Tuple;
//testng
import org.testng.Assert;
import org.testng.annotations.*
import static org.testng.Assert.*;

//json
import groovy.json.JsonSlurper
import groovy.util.logging.*

@Slf4j
class GroovyPOCTest {
  @BeforeClass(groups = ["acceptance"])
  def setUp() {
  }

  @Test(groups = ["acceptance"])
  void rake(){
  }
  
  @AfterClass(groups = ["acceptance"])
  def reportingAndCleanUp(){
  }
  
}

