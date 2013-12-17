package water.api.dsl;

import water.MRTask2;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;

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
		}.doAll(fr.numCols(), fr).outputFrame(fr.names(), fr.domains());

		return r;
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
		}.doAll(fr.numCols(), fr).outputFrame(fr.names(), fr.domains());;

		return r;
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
		}.doAll(fr.numCols(), fr).outputFrame(fr.names(), fr.domains());

		return r;
	}
	
	public static Frame pow(Frame fr, final double d) {
		Frame r = new MRTask2() {
			@Override
			public void map(Chunk[] cs, NewChunk[] ncs) {
				for (int i = 0; i < ncs.length; i++) {
					NewChunk nc = ncs[i];
					Chunk c = cs[i];
					for (int r = 0; r < c._len; r++)
						nc.addNum(Math.pow(c.at0(r), d));
				}
			}
		}.doAll(fr.numCols(), fr).outputFrame(fr.names(), fr.domains());

		return r;
	}
		
}
