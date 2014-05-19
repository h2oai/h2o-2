package cookbook;

import java.io.File;

import org.junit.Test;

import water.Key;
import water.MRTask2;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.util.Log;
/*
 * This example fills na's in a column with the column mean by traversing over each column in the map reduce call
 * and adding the new mean filled out columns to the existing frame
 */
public class FillNAsWithMeanDemo01 extends AbstractCookbook{ 
	@Test
    public void frame_001() {
		//String fileName = "/Users/nidhimehta/h2o/smalldata/iris/iris.csv";
		//String fileName = "/Users/nidhimehta/Desktop/data/covtype/covtrain_tit";
		//String fileName = "/Users/nidhimehta/Desktop/iris_withNA.csv";
		String fileName = "./cookbookData/iris_withNA.csv";
		
		File file = new File(fileName);
		Key fkey = NFSFileVec.make(file);
		Key okey = Key.make("iris.hex");		
		
		Frame fr;
		fr = ParseDataset2.parse(okey, new Key[]{fkey});
		int len = fr.numCols();
		for(int i=0; i<len; i++){
			Vec vv = fr.vec(i);
			Vec output = vv.makeZero(); // creating a new vector same as original vector filled with zeros 
			FillNasWithMean lr1 = new FillNasWithMean(vv.mean()).doAll(vv, output);// map reduce call
			fr.add("FilledNa"+i,output ); // adding the vector to the original frame
		}
		Log.info("frame              : " + fr);
		//logThisH2OInstanceWebBrowserAddress();
        //sleepForever();

        Frame.delete(okey);
	}
	public static class FillNasWithMean extends MRTask2<FillNasWithMean>{
		 final double _meanX;
		 FillNasWithMean( double meanX ) { 
			 _meanX = meanX; 
		 }
		 @Override public void map( Chunk xs, Chunk ns) {
			 for( int l=0; l<xs._len; l++ ) {
			    double X = xs.at0(l);
		        if(  xs.isNA0(l)) {
			      	X = _meanX;
			       	ns.set0(l,_meanX);
		        }
		        else{
		        	ns.set0(l,X);
		        }
			 }
		 }
	}
}	