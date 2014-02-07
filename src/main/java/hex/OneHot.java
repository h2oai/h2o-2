package hex;

import water.MRTask2;
import water.Key;
import water.fvec.*;

import java.util.Arrays;
import java.util.ArrayList;

public class OneHot extends MRTask2<OneHot>{
    int[] _offsets;

    public static Frame expandDataset(Frame fr, Key destkey) {//, int[] ignored) {
        ArrayList<Vec> nvecs = new ArrayList<Vec>();
        ArrayList<Vec> evecs = new ArrayList<Vec>();
        ArrayList<String> eNames = new ArrayList<String>();
        ArrayList<String> nNames = new ArrayList<String>();
        int[] offsets = new int[fr.numCols()+1];
        Vec[] vecs = fr.vecs();
        int c = 0;
       // int ip = 0; //ignored pointer
        for (int i = 0; i < fr.numCols(); i++) {
            if( vecs[i].isEnum() ) {//&& i != ignored[ip]) {//!fr._names {//_names[i]. { //equals(ignored)) {
                offsets[evecs.size()] = c;
                evecs.add(vecs[i]);
                String name = fr._names[i];
                c += vecs[i]._domain.length;
                for(String s: vecs[i]._domain) eNames.add(name+"."+s);
            } else {
                //if(i == ignored[ip] && ip < ignored.length - 1) ip++;
                nvecs.add(vecs[i]);
                nNames.add(fr._names[i]);
            }
        }
        offsets[evecs.size()] = c;
        if (evecs.isEmpty()) return fr;
        offsets = Arrays.copyOf(offsets, evecs.size() + 1);

        OneHot ss = new OneHot();
        ss._offsets = offsets;
        int l = offsets[evecs.size()];
        ss.doAll(l,evecs.toArray(new Vec[evecs.size()]));


        Frame fr2 = ss.outputFrame(destkey,eNames.toArray(new String[eNames.size()]),new String[l][]);
        fr2.add(new Frame(nNames.toArray(new String[nNames.size()]), nvecs.toArray(new Vec[nvecs.size()])),false);
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
}
