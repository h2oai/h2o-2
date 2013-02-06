package water;

import java.util.Arrays;

/**
 * Atomic Append of a Key
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class AppendKey extends Atomic {
  private Key _key;

  public AppendKey( Key keyToAppend ) {
    _key = keyToAppend;
  }

  // Just append the bits
  @Override public byte[] atomic( byte[] bits1 ) {
    Key[] n;
    if( bits1 == null ) {
      n = new Key[] { _key };
    } else {
      AutoBuffer old = new AutoBuffer(bits1);
      Key[] k = old.getA(Key.class);
      n = Arrays.copyOf(k, k.length+1);
      n[k.length] = _key;
    }
    return new AutoBuffer().putA(n).buf();
  }

  // Do not return the bits, so on success zap the array.
  @Override public void onSuccess() { _key = null; }
}
