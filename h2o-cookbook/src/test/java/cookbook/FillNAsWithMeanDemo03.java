package cookbook;

import java.io.File;

import org.junit.Test;

import water.DKV;
import water.Futures;
import water.Key;
import water.MRTask2;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.NewChunk;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.util.Log;
/*
 * This example fills na's in a column with the column mean and creates new columns 
 * by traversing over the original frame in the map reduce call
 * and the new mean filled out columns are then added to a new data frame
 */
public class FillNAsWithMeanDemo03 extends AbstractCookbook {
	@Test
	public void frame_001() {
		// String fileName = "/Users/nidhimehta/h2o/smalldata/iris/iris.csv";
		 //String fileName = "/Users/nidhimehta/Desktop/data/covtype/covtrain_tit";
		//String fileName = "/Users/nidhimehta/Desktop/iris_withNA.csv";
		String fileName = "./cookbookData/iris_withNA.csv";
		
		File file = new File(fileName);
		Key fkey = NFSFileVec.make(file);
		Key okey = Key.make("iris.hex");

		Frame fr;
		fr = ParseDataset2.parse(okey, new Key[] { fkey });
		
		Frame f = DKV.get(okey).get();
		int len = f.numCols();
		Vec vv[] = f.vecs();
		double[] arrayofMeans = new double[len];

		for (int i = 0; i < len; i++)
			arrayofMeans[i] = vv[i].mean();

		
		FillNasWithMean lr1 = new FillNasWithMean(arrayofMeans).doAll(len, f); // map reduce call
		
		Key fk = Key.make(f._key.toString() + "_nas_replaced_with_mean");
		
		Futures fs = new Futures(); 
		Frame outputFrame = lr1.outputFrame(fk, f.names(), f.domains(),fs); //new frame
		fs.blockForPending();
		
		DKV.put(fk,outputFrame,fs); //puts the new frame in the KV store
		
		fs.blockForPending();
		
		Log.info(" new output frame        : " + outputFrame);
		//logThisH2OInstanceWebBrowserAddress();
        //sleepForever();

        Frame.delete(okey);
        outputFrame.delete();
    }
	public static class FillNasWithMean extends MRTask2<FillNasWithMean> {
		final double[] _meanX;

		FillNasWithMean(double[] meanX) {
			_meanX = meanX;
		}

		@Override
		public void map(Chunk[] xs, NewChunk[] ns) {
			for (int j = 0; j < xs.length; j++) {
				for (int l = 0; l < xs[j]._len; l++) {
					if (xs[j].isNA0(l)) {
						ns[j].addNum(_meanX[j]);				
					} else {
						ns[j].addNum(xs[j].at0(l));
					}
				}
			}
		}
	}
}	
