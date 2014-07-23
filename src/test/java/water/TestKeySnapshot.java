package water;

import java.util.Map;

import org.junit.Test;

import junit.framework.Assert;
import water.util.Utils;

/**
 * Created by tomasnykodym on 5/30/14.
 */
public class TestKeySnapshot extends TestUtil {
  @Test
  public void testGlobalKeySet(){
    try {
      Futures fs = new Futures();
      for (int i = 0; i < 100; ++i)
        DKV.put(Key.make("key" + i), new Utils.IcedInt(i),fs,true);
      for (int i = 0; i < 100; ++i)
        DKV.put(Key.makeUserHidden(Key.make()), new Utils.IcedInt(i),fs,true);
      fs.blockForPending();
      Key[] keys = H2O.KeySnapshot.globalSnapshot().keys();
      Assert.assertEquals(100, keys.length);
    } finally {
      for (int i = 0; i < 100; ++i) {
        DKV.remove(Key.make("key" + i));
        DKV.remove(Key.makeUserHidden(Key.make()));
      }
    }
  }

  @Test
  public void testLocalKeySet(){
    Key [] userKeys = new Key[100];
    Key [] systemKeys = new Key[100];
    int homeKeys = 0;
    Futures fs = new Futures();
    try {
      for(int i = 0; i < userKeys.length; ++i){
        DKV.put(userKeys[i] = Key.make("key" + i), new Utils.IcedInt(i),fs,true);
        if(userKeys[i].home())++homeKeys;
        DKV.put(systemKeys[i] = Key.makeUserHidden(Key.make()), new Utils.IcedInt(i),fs,true);
      }
      fs.blockForPending();
      Key[] keys = H2O.KeySnapshot.localSnapshot().keys();
      Assert.assertEquals(homeKeys, keys.length);
      for (Key k:keys)
        Assert.assertTrue(k.home());
    } finally {
      for (int i = 0; i < userKeys.length; ++i) {
        DKV.remove(userKeys[i]);
        DKV.remove(systemKeys[i]);
      }
    }
  }

  @Test
  public void testFetchAll(){
    Key [] userKeys = new Key[200];
    Key [] systemKeys = new Key[200];
    int homeKeys = 0;
    Futures fs = new Futures();
    try {
      for(int i = 0; i < (userKeys.length >> 1); ++i){
        DKV.put(userKeys[i] = Key.make("key" + i), new Utils.IcedInt(i),fs,false);
        if(userKeys[i].home())++homeKeys;
        systemKeys[i] = Key.makeUserHidden(Key.make());
        DKV.put(systemKeys[i], new Value(systemKeys[i], new Utils.IcedInt(i)));
      }
      for(int i = (userKeys.length >> 1); i < userKeys.length; ++i){
        DKV.put(userKeys[i] = Key.make("key" + i), new Utils.IcedDouble(i),fs,false);
        if(userKeys[i].home())++homeKeys;
        systemKeys[i] = Key.makeUserHidden(Key.make());
        DKV.put(systemKeys[i], new Value(systemKeys[i], new Utils.IcedDouble(i)));
      }
      fs.blockForPending();
      H2O.KeySnapshot s = H2O.KeySnapshot.globalSnapshot();
      Map<String,Iced> all =  s.fetchAll(Iced.class,true);
      Assert.assertTrue(all.isEmpty());
      all =  s.fetchAll(Iced.class);
      Assert.assertEquals(userKeys.length, all.size());
      Map<String,Utils.IcedInt> ints =  s.fetchAll(Utils.IcedInt.class);
      Map<String,Utils.IcedDouble> doubles =  s.fetchAll(Utils.IcedDouble.class);
      Assert.assertEquals(userKeys.length >> 1, ints.size());
      Assert.assertEquals(userKeys.length >> 1, doubles.size());
    } finally {
      for (int i = 0; i < userKeys.length; ++i) {
        if(userKeys[i]   != null)DKV.remove(userKeys[i]);
        if(systemKeys[i] != null)DKV.remove(systemKeys[i]);
      }
    }
  }

}
