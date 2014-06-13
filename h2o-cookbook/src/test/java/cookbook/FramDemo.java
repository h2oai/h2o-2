package cookbook;

import java.io.File;

import org.junit.Test;

import water.Key;
import water.UKV;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;

public class FramDemo extends AbstractCookbook{
	@Test
	public void Frame_1(){
		
		String fileName = "./cookbookData/cars_nice_header.csv";
		
		File file = new File(fileName);
		Key fkey = NFSFileVec.make(file);
		Key okey = Key.make("cars.hex");	
		

		Frame fr;
		fr = ParseDataset2.parse(okey, new Key[]{fkey});
		
		// ACCESSING A VEC FROM A FRAME
		Vec vv = fr.vec(0);                 // by index
		System.out.println(vv);            	// prints summary of the vec 
		Vec vvc = fr.vec("name");			// by column name	
		System.out.println(vvc); 	
		Vec allVec[] = fr.vecs();			// all vectors
		System.out.println("Number of vectors: "+allVec.length);
		
		//PRINTING ALL COLUMN (AKA VEC) NAMES IN A FRAME
		String colNames[] = fr.names();
		for(int i=0; i<colNames.length;i++)
		System.out.println("Name of vector "+i + ": "+colNames[i]);
	
		//ADDING AN EXISTING VEC TO A FRAME
		System.out.println("Number of vectors in original frame: "+fr.numCols());		//checking number of columns in the frame
		fr.add("Added_vector", vv);									//added an existing vector 
		System.out.println("Added a vector: "+fr);
		
		//CREATING A NEW FRAME WITH A SUBSET OF VECS FROM AN EXISTING FRAME
		String[] colTosubset = {"name","economy"};
		Frame Sfr = fr.subframe(colTosubset);		//by specifying column names
		System.out.println("Subframe made by specifying colnames:"+Sfr);
		//here, the sub frame is not added to the kv store
		
		Frame Sfr2 = fr.subframe(0, 3);				// by specifying start and end indices(excludes end index)
		System.out.println("Subframe made by specifying indices:"+Sfr2);
				
		String colTosubsetOn[] = {"name","economy","newvec1","newvec2"};
		Frame Sfr3[] = fr.subframe(colTosubsetOn, 0);// creates two frames- First frame contains all columns from existing frame and the new vectors filled with specified double value
															   // Second frame contains columns missing in the original frame and are filled with the specified constant value		
		System.out.println("Subframe from original frame with new constant vecs: "+Sfr3[0]);
		System.out.println("Newframe with constant vecs: "+Sfr3[1]);
				
		
		// REMOVING A VEC FROM A FRAME
		int colToRemove = 8;					
		String name = fr.names()[colToRemove];
		fr.remove(colToRemove); 		// by specifying index
		/*Waring: this command only removes the vector reference leaving behind the data in the KV store.
		 *Useful when a vector is owned by many frames and want to delete it from only one frame.
		 *Works here because vector 8 is a copy of the 1st vector in the original frame 
		*/ 
		System.out.println("Frame after column "+ name +" removed : "+ fr);		
		
		/* fr.remove("weight");						   // by specifying name
		 * System.out.println("Frame after  column weight is removed: "+ fr);
		 *
		 * int idxsToremove[] ={6,7};					
		 * fr.remove(idxsToremove);						//removes multiple columns by specifying indices
		 * System.out.println("Frame after "+idxsToremove.length +" columns are removed: "+ fr);
		 * 
		 * fr.remove(0, 2);								   // by specifying start and end indices(excludes end index)
		 * System.out.println("Frame after  specified range of columns are removed: "+ fr);
		 */	
		
		//REMOVING ALL FRAME REFERENCES TO A VEC AND RECLAIMING ITS MEMORY
		UKV.remove(fr.remove("cylinders")._key);
		System.out.println("Frame after  column 'cylinders' is removed: "+ fr);
		
		//CREATING A NEW DOUBLE VEC FROM NOTHING AND ADDING IT TO A FRAME
		/*This is efficient only when a small/few vectors needs to be generated. 
		 * Otherwise use mapreduce
		 */
		Vec dv = fr.anyVec().makeZero();
		Vec.Writer vw = dv.open();
			for (long i = 0; i < dv.length(); ++i)
				vw.set(i, (double)i+0.1);
	    vw.close();
	    fr.add("New_Double_Vec", dv);
		
		
		//CREATING A NEW LONG VEC FROM NOTHING AND ADDING IT TO A FRAME
	    Vec lv = fr.anyVec().makeZero();
		Vec.Writer lvw = lv.open();
			for (long i = 0; i < lv.length(); ++i)
				lvw.set(i, i);
	    lvw.close();
	    fr.add("New_Long_Vec", lv);
		
		//CREATING A NEW ENUM VEC FROM NOTHING AND ADDING IT TO A FRAME
		final String [] Domain = {"a", "b", "c","d"};
	    Vec ev = fr.anyVec().makeCon(lv.length(), Domain);
	    Vec.Writer evw = ev.open();
	       for (long i = 0; i < ev.length(); ++i)
	         evw.set(i, i % 4);
	    evw.close();
	    fr.add("NewEnumvec", ev);
	       
	    //logThisH2OInstanceWebBrowserAddress();
	    //sleepForever();
	    
	    //CLEANING THE KV STORE OF ALL DATA
	    Frame.delete(okey);
	    Sfr3[1].delete();
	    Sfr3[0].delete();
	    Sfr2.delete();
	    Sfr.delete();
	    //DKV.remove(okey);
	}

}
