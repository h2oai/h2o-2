package cookbook;

import java.io.File;

import org.junit.Test;

import water.DKV;
import water.Key;
import water.Value;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.util.Log;

/*
 * This example parses a frame into H2O and prints the various specifics of all vectors and chunks
 */
public class VecChunkDemo { 
	@Test
    public void frame_001() {
		
		String fileName = "../smalldata/iris/iris.csv";
		
		File file = new File(fileName);
		Key fkey = NFSFileVec.make(file);
		Key okey = Key.make("iris.hex");	
		

		Frame fr;
		fr = ParseDataset2.parse(okey, new Key[]{fkey});
		
		Value v = DKV.get(okey);
		Frame f = v.get();
		Log.info("frame              : "+f);
		int len = f.numCols(); 
		for(int i=0; i<len; i++){
			Log.info("vector                        :" +i);
			Vec vv = f.vec(i); // looping through the vectors of a frame and printing specifics
			
			Log.info("vector     summary                :" + vv);
			Log.info("vector     length                 :"+vv.length());
			Log.info("vector     group                  :"+vv.group()); 
			Log.info("vector     na count               :" + vv.naCnt());
			Log.info("vector     domain null if not enum:"+vv.domain()); // null if not enum
			int cardinality = vv.cardinality();
			Log.info("vector     cardianlity            :"+vv.cardinality());
			if(cardinality != -1){
				for(int j = 0; j<cardinality; j++)
					Log.info("labels                    :" +vv.domain(j) );
			}
			Log.info("vector value at row 50            :"+vv.at(51)); //gives the element at that row; count starts from 0.
			
			
			int chunk_count = vv.nChunks();
			Log.info("chunk     count                   :"+ chunk_count); 
			Chunk c = vv.chunkForRow(100);
			Log.info("chunk     for row 100             :"+ c); 
			
			
			
		}
		
	}
}	