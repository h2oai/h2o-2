package cookbook;

import java.io.File;
import org.junit.Test;
import water.DKV;
import water.Key;
import water.MRTask2;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.util.Log;
/*
 * This example fills na's in a column with the column mean and creates new columns 
 * and add the new mean filled out columns to the original data frame that is passed 
 * to the map reduce call with the required placeholder columns
 */
public class FillNAsWithMeanDemo02 extends AbstractCookbook {
	@Test
	public void frame_001() {
		
		String fileName = "./cookbookData/iris_withNA.csv";
		//String fileName = "/Users/nidhimehta/Desktop/iris_withNA.csv";

		File file = new File(fileName);
		Key fkey = NFSFileVec.make(file);
		Key okey = Key.make("iris.hex");

		Frame fr;
		fr = ParseDataset2.parse(okey, new Key[] { fkey });

		Frame f = DKV.get(okey).get();
		Log.info("frame              : " + f);

		int len = f.numCols();
		Vec vv[] = f.vecs();
		
		double[] arrayofMeans = new double[len];
		for (int i = 0; i < len; i++)
			arrayofMeans[i] = vv[i].mean();         // array of means to be passed as params to map reduce task

		Vec[] newVecs = vv[0].makeZeros(len);
		newVecs[4]._domain= vv[4]._domain;
		String[] newcolnames = {"1","2","3","4","5"};
		Frame output = frame(newcolnames, newVecs);
		
		f.add(output, newcolnames);                // the holder frame added to original frame
		
		FillNasWithMean lr1 = new FillNasWithMean(arrayofMeans).doAll(f); // map reduce call
		
		Log.info("frame              : " + f);
		
		//logThisH2OInstanceWebBrowserAddress();
        //sleepForever();
        Frame.delete(okey);
    }
	public static class FillNasWithMean extends MRTask2<FillNasWithMean> {
		final double[] _meanX;

		FillNasWithMean(double[] meanX) {
			_meanX = meanX;
		}

		@Override
		public void map(Chunk[] xs) {
			for (int j = 0; j < xs.length/2; j++) {
				for (int l = 0; l < xs[j]._len; l++) {
					if (xs[j].isNA0(l)) {
						xs[j+xs.length/2].set0(l,_meanX[j]);
						// xs.set0(l, _meanX);
						// System.out.println("hello hello");						
					} else {
						xs[j+xs.length/2].set0(l,xs[j].at0(l));
					}
				}
			}
		}
	}
}	
