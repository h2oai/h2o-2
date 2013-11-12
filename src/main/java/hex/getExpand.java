package hex;

import water.Key;
import water.fvec.NewChunk;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.MRTask2;
import water.fvec.*;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.io.File;
import water.DKV;
import water.*;


public class getExpand extends MRTask2<getExpand>{
  int[] _offsets;

  public static Frame expandDataset(Frame fr, String response) {
    ArrayList<Vec> nvecs = new ArrayList<Vec>();
    ArrayList<Vec> evecs = new ArrayList<Vec>();
    ArrayList<String> eNames = new ArrayList<String>();
    ArrayList<String> nNames = new ArrayList<String>();
    int[] offsets = new int[fr.numCols()+1];
    Vec[] vecs = fr.vecs();
    int c = 0;
    for (int i = 0; i < fr.numCols(); i++) {
      if( vecs[i].isEnum() && !fr._names[i].equals(response)) {
        offsets[evecs.size()] = c;
        evecs.add(vecs[i]);
        String name = fr._names[i];
        c += vecs[i]._domain.length;
        for(String s: vecs[i]._domain) eNames.add(name+"."+s);
      } else {
        nvecs.add(vecs[i]);
        nNames.add(fr._names[i]);
      }

    }
    offsets[evecs.size()] = c;
    if (evecs.isEmpty()) return fr;
    offsets = Arrays.copyOf(offsets, evecs.size() + 1);

    getExpand ss = new getExpand();
    ss._offsets = offsets;
    int l = offsets[evecs.size()];
    ss.doAll(l,evecs.toArray(new Vec[evecs.size()]));


    Frame fr2 = ss.outputFrame(eNames.toArray(new String[eNames.size()]),new String[l][]);
    fr2.add(new Frame(nNames.toArray(new String[nNames.size()]), nvecs.toArray(new Vec[nvecs.size()])));
    return fr2;
  }

  @Override public void map(Chunk[] inputs, NewChunk[] outputs) {
    for(int i=0; i <inputs[0]._len; i++ ) {
      for(int j=0; j<inputs.length; j++) {
        int idx = (int)inputs[j].at0(i);
        for(int k = 0; k <( _offsets[j+1] - _offsets[j]); k++) {
          outputs[k+_offsets[j]].addNum(k==idx ? 1 : 0, 0);
        }
      }
    }
  }

  public static void main(String [] args) throws Exception {
    int nnodes = Integer.valueOf(args[0]);
    File file = new File(args[1]);
    if (!file.exists()) {
        System.err.println("File not found "+ file.getAbsolutePath());
        System.exit(-1);
    }
    String response = args[2];
    H2O.waitForCloudSize(nnodes);
    Key frHex = Key.make("fr.hex");
    Key frRaw = NFSFileVec.make(file);
    ParseDataset2.parse(frHex, new Key[]{frRaw});

    Frame fr = DKV.get(frHex).get();
    Key resHex = Key.make(args[1] + "expanded.hex");
    Frame res = getExpand.expandDataset(fr, response);
    InputStream is = res.toCSV(true);
    File output = new File(file.getAbsolutePath() + "_expanded.csv");
    FileOutputStream out = new FileOutputStream(output);
    try {
      byte[] buff = new byte[32*1024];
      int n = 0;
      while((n = is.read(buff)) != -1)
        out.write(buff,0,n);
    } finally {
        out.close();
    }
  }
}
