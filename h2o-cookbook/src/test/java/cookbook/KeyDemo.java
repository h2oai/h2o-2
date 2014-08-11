package cookbook;

import java.io.File;

import org.junit.Test;

import water.DKV;
import water.H2O;
import water.Key;
import water.UKV;
import water.Value;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.util.Log;
/*
 * This example parses a data frame, prints key counts and then deletes the data frame from the KV store 
 */
public class KeyDemo {
	@Test
    public void frame_001() {
	    int initial_keycnt0 = H2O.store_size();
		Log.info("initial key count: should be One builtin jobkey and probably a log key:             "+initial_keycnt0);
		//Log.info(H2O.STORE.toString());	
		
		//String fileName = "/Users/nidhimehta/Desktop/data/covtype/covtrain_tit";
		String fileName = "../smalldata/iris/iris.csv";

	   
		File file = new File(fileName);
		Key fkey = NFSFileVec.make(file);
		Key okey = Key.make("iris.hex");		
		//Expected: fkey holds the info before parse and is null after parse, as it passes the data to 
	    // okey which is null before parse. 
		
       // int initial_keycnt = H2O.store_size();
		Log.info("NFSFile key added to the key store, so should be plus one:  "+H2O.store_size());
		//Log.info(H2O.STORE.toString());
		
		Log.info("UKV fkey before parse:" + UKV.get(fkey));
		Log.info("DKV fkey before parse:" + DKV.get(fkey));
		Log.info("UKV okey before parse:" + UKV.get(okey));
		Log.info("DKV okey before parse:" + DKV.get(okey));
		
		Frame fr;
		fr = ParseDataset2.parse(okey, new Key[]{fkey});
		
		Log.info("-------After parse of file ---------");
        
		Log.info("key count after frame parse: (5)vectors, (5)chunks, (1)vector group keys  + 2 keys (-1 NFSkey):  "+ H2O.store_size());
		//Log.info(H2O.STORE.toString());
		
		Log.info("UKV fkey after parse:" + UKV.get(fkey));
		Log.info("DKV fkey after parse:" + DKV.get(fkey));
		Log.info("UKV okey after parse:" + UKV.get(okey));
		Log.info("DKV okey after parse:" + DKV.get(okey));
		Log.info("DKV okey get        :" + DKV.get(okey).get());

        H2O.KeySnapshot ks = H2O.KeySnapshot.globalSnapshot();
        long keyCount = ks.keys().length;
		Log.info("Global Keyset count :" + keyCount);
		Log.info("Sanity check:key count after a few prints should not change anything;But not so straightforward if more than 1 nodes: "+H2O.store_size());
		//Log.info(H2O.STORE.toString());
		
	    //UKV.remove(okey); //this does not work use frame.delete cascade deletes the stuff
		//DKV.remove(okey);// this will remove just the header and that's all
		Frame.delete(okey);
		//Log.info(H2O.STORE.toString());
		try { Thread.sleep(3000); } catch(InterruptedException _) {} ;
		Log.info("After frame delete, just the job key, builtin job key, (and probably a log key and the vector group key) should remain: "+ H2O.store_size());
		//Log.info(H2O.STORE.toString());
    }
}