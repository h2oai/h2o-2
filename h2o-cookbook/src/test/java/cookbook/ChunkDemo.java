package cookbook;

import java.io.File;

import org.junit.Test;

import water.Key;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.fvec.Chunk;

public class ChunkDemo extends AbstractCookbook{
	@Test
	public void Chunk(){
		String fileName = "./cookbookData/iris_withNA.csv";
		File file = new File(fileName);
		Key fkey = NFSFileVec.make(file);
		Key okey = Key.make("iris.hex");	
		
		Frame fr;
		fr = ParseDataset2.parse(okey, new Key[]{fkey});
		Vec vv = fr.vec(0);   //accessing the first vector from the frame
		int chunk_num = vv.nChunks();
		System.out.println("Number of chunks in column 1:  "+chunk_num);
		
		Chunk cc = vv.chunkForChunkIdx(0);  //Reading in the first chunk. This loads the data locally.
		
		for(int i=0; i < cc._len;i++){
		//READING A DOUBLE ELEMENT FROM A CHUNK
		double d_at = cc.at0(i);       // at0 gives the local chunk index
		System.out.println("double Value at chunk index "+i+ ":  "+d_at);
		//READING A LONG ELEMENT FROM A CHUNK
		if(!Double.isNaN(d_at)){
			long l_at = cc.at80(i);
			System.out.println("long Value at chunk index "+i+ ":  "+l_at);
		}	
		//UPDATING A DOUBLE ELEMENT TO A CHUNK
		double d = 1.23;
		double set_dval = cc.set0(i, d);
		System.out.println("Setting a double value at index "+i+" : "+set_dval);
		//UPDATING A LONG ELEMENT TO A CHUNK
		long l = 123L;
		long set_lval = cc.set0(i, l);
		System.out.println("Setting a double value at index "+i+" : "+set_lval);
		}
		
		//logThisH2OInstanceWebBrowserAddress();
	    //sleepForever();	
		
		//CLEANING THE KV STORE OF ALL DATA
		Frame.delete(okey);
	}

}
