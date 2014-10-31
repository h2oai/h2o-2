package h2o_proof;

/**
 * Created by radu on 8/21/14.
 */

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class POCProofTest {

    @BeforeClass
    public void initTest() throws Exception {
    }

    @Test(groups = {"acceptance"})
    public void proofOfConceptTest() throws Exception {
        System.out.println("Here is the POC that TestNG ran ...");
    }

}
