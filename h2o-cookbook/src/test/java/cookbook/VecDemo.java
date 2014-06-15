package cookbook;

import java.io.File;
import java.util.Arrays;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import water.Key;
import water.MRTask2;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.util.Log;

public class VecDemo extends AbstractCookbook{
	@Test
	public void Vec(){
		
		String fileName = "./cookbookData/iris_withNA.csv";
		File file = new File(fileName);
		Key fkey = NFSFileVec.make(file);
		Key okey = Key.make("iris.hex");	
		
		Frame fr;
		fr = ParseDataset2.parse(okey, new Key[]{fkey});
		
		Vec vv = fr.vec(0);   //accessing the first vector from the frame
	    
		int loop_indx = 0;		
			if(vv.length() > 4){
				loop_indx = 3;
			}
		
		//READING AN ELEMENT FROM A VEC
		for(int i =0; i<loop_indx;i++){
			long k =i;
			double elemnt_D = vv.at(k); // element at index k returns a double
			System.out.println("element at index " + k +" as double: "+elemnt_D);
			if(!Double.isNaN(elemnt_D)){
				long elemnt_L = vv.at8(k); // element at index k returns an (rounded) int, throws if a value is missing
				System.out.println("element at index " + k +" as integer: "+elemnt_L);
			}	
			
			
		}
 
	    //TESTING WHETHER A VEC OF INTEGERS IS AN ENUM (AKA CATEGORICAL) OR NOT
		for(int i = 0; i<fr.numCols();i++){
			Vec vvec = fr.vec(i);  
			boolean b= vvec.isInt();	//check if int
			System.out.println("Is "+ i +" an integer column ?" +"  "+b);
			if(b==true){
				int cardinality = vvec.cardinality(); // check if enum
				if(cardinality !=-1){
					System.out.println("The vector " + i +" is an enum with cardinality "+ 
							cardinality+ " and domain names: ");
					//PRINTING THE LIST OF DOMAINS OF AN ENUM VEC (AKA LEVELS OF A CATEGORICAL VEC)
					for(int j = 0; j<cardinality; j++)
						System.out.println( vvec.domain(j) );
				}
			} 
		 }	

		
		//UPDATING AN ELEMENT OF A VEC
		/* This sets the value in a very slow way, because it takes the vector goes to the chunk that has 
		 * the row index, decompress it, updates the value and then compress it again
		*/
		for(int i = 0; i<loop_indx;i++){
			long k = i;
			double d = 1.23;
			vv.set(k, d);  // set element as double
			System.out.println("setting element at index " + k +" as double: "+vv.at(k));
			float f = 1.23f;
			vv.set(k, f);	// set element as float
			System.out.println("setting element at index " + k +" as float: "+vv.at(k));
			long l = 12345678910L;
			vv.set(k, l);	// set element as long
			System.out.println("setting element at index " + k +" as long: "+vv.at(k));
					
			vv.setNA(k);	// set element as na
			System.out.println("setting element at index " + k +" as NAN: "+vv.at(k));
		}
		
		//UPDATING A VEC ELEMENT WITH AN ENUM VALUE THAT HAS NEVER BEEN USED BEFORE
		Vec vvenum = fr.vec(4);
		final String [] newDomain = new String[]{"x", "y", "z"};
		vvenum.changeDomain(newDomain);
		System.out.println( "The changed domain names are: ");
		for(int i = 0; i<vvenum.cardinality(); i++)
			System.out.println( vvenum.domain(i) );
		//fr.vec(4).changeDomain(newDomain);
		
		//ACCESSING VEC STATS THAT ARE COMPUTED AUTOMATICALLY (LIKE MIN, MAX)
		System.out.println("Min for vector 0: "+vv.min());
		System.out.println( "Max for vector 0: "+vv.max());
		System.out.println( "Mean for vector 0: "+vv.mean());
		System.out.println( "Standard deviation for vector 0: "+vv.sigma());
		System.out.println( "NA count for vector 0: "+vv.naCnt());
						
		//logThisH2OInstanceWebBrowserAddress();
	    //sleepForever();	
		
		//CLEANING THE KV STORE OF ALL DATA
		Frame.delete(okey);
	}	
	
}
