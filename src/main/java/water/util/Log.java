package water.util;

abstract class Log {

	public static interface Tag {
		public static enum Subsystem implements Tag {
		RF, GLM, KMEANS, PARSE, STORE;
		}
	}

	/** Verbosity for debug log level */
	static private int level = Integer.getInteger("h2o.log.debug.level", 1);

	static public Throwable fatal(Object _this, Tag t, String msg, Throwable exception) {
		// some printing
		return exception;
	}

	static public void warn(Object _this, Tag t, String msg) {

	}

	static public void info(Object _this, Tag t, Object... objects) {

	}

	static public void debug(Object _this, Tag t, Object... objects) {

	}

	static public void debug1(Object _this, Tag t, Object... objects) {
		if (level < 1) return;

	}

	static public void debug2(Object _this, Tag t, Object... objects) {
		if (level < 2) return;
	}

	static public void debug3(Object _this, Tag t, Object... objects) {
		if (level < 3) return;

	}

	static public class Message {
		String threadId() {
			return null;
		}

		String nodeId() {
			return null;
		}

		String jobId() {
			return null;
		}

		String sourceClass() {
			return null;
		}

		String tag() {
			return null;
		}

		String message() {
			return null;
		}

		String exception() {
			return null;
		}

		public String toString() {
			return null;
		}

		static public Message parse(String s) {
			return null;
		}
	}
}
