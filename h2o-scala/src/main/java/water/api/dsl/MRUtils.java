package water.api.dsl;

import water.DKV;
import water.Futures;
import water.MRTask2;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.Vec;

public class MRUtils {

	public static Frame sub(Frame fr, final double d) { return add(fr, -d); }
	public static Frame add(Frame fr, final double d) {
		Frame r = new MRTask2() {
			@Override
			public void map(Chunk[] cs, NewChunk[] ncs) {
				for (int i = 0; i < ncs.length; i++) {
					NewChunk nc = ncs[i];
					Chunk c = cs[i];
					for (int r = 0; r < c._len; r++)
						nc.addNum(c.at0(r) + d);
				}
			}
		}.doAll(fr.numCols(), fr)._outputFrame;

		return copyHeaders(fr, r, null);
	}
	
	public static Frame mul(Frame fr, final double d) {
		Frame r = new MRTask2() {
			@Override
			public void map(Chunk[] cs, NewChunk[] ncs) {
				for (int i = 0; i < ncs.length; i++) {
					NewChunk nc = ncs[i];
					Chunk c = cs[i];
					for (int r = 0; r < c._len; r++)
						nc.addNum(c.at0(r) * d);
				}
			}
		}.doAll(fr.numCols(), fr)._outputFrame;

		return copyHeaders(fr, r, null);
	}
	public static Frame div(Frame fr, final double d) {
		Frame r = new MRTask2() {
			@Override
			public void map(Chunk[] cs, NewChunk[] ncs) {
				for (int i = 0; i < ncs.length; i++) {
					NewChunk nc = ncs[i];
					Chunk c = cs[i];
					for (int r = 0; r < c._len; r++)
						if (d!=0) nc.addNum(c.at0(r) * d); else nc.addNA();
				}
			}
		}.doAll(fr.numCols(), fr)._outputFrame;

		return copyHeaders(fr, r, null);
	}
	

	// Copy over column headers & enum domains from self into fr2
	public static Frame copyHeaders(Frame ffr, Frame tfr, int cols[]) {
		Futures fs = new Futures();
		Vec[] vec2 = tfr.vecs();
		String domains[][] = ffr.domains();
		int len = cols == null ? vec2.length : cols.length;
		String ns[] = new String[len];
		for (int i = 0; i < len; i++) {
			ns[i] = ffr._names[cols == null ? i : cols[i]];
			vec2[i]._domain = domains[cols == null ? i : cols[i]];
			DKV.put(vec2[i]._key, vec2[i], fs);
		}
		tfr._names = ns;
		fs.blockForPending();
		return tfr;
	}
}
