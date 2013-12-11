package water.util;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Data structure for holding network info specified by the user on the command line.
 */
public class UserSpecifiedNetwork {
  int _o1;
  int _o2;
  int _o3;
  int _o4;
  int _bits;

  /**
   * Create object from user specified data.
   * @param o1 First octet
   * @param o2 Second octet
   * @param o3 Third octet
   * @param o4 Fourth octet
   * @param bits Bits on the left to compare
   */
  public UserSpecifiedNetwork(int o1, int o2, int o3, int o4, int bits) {
    _o1 = o1;
    _o2 = o2;
    _o3 = o3;
    _o4 = o4;
    _bits = bits;
  }

  private boolean oValid(int o) {
    if (o < 0) return false;
    if (o > 255) return false;
    return true;
  }

  private boolean valid() {
    if (! (oValid(_o1))) return false;
    if (! (oValid(_o2))) return false;
    if (! (oValid(_o3))) return false;
    if (! (oValid(_o4))) return false;
    if (_bits < 0) return false;
    if (_bits > 32) return false;
    return true;
  }

  /**
   * Test if an internet address lives on this user specified network.
   * @param ia Address to test.
   * @return true if the address is on the network; false otherwise.
   */
  public boolean inetAddressOnNetwork(InetAddress ia) {
    int i = (_o1 << 24) |
            (_o2 << 16) |
            (_o3 << 8) |
            (_o4 << 0);

    byte[] barr = ia.getAddress();
    if (barr.length != 4) {
      return false;
    }

    int j = (((int)barr[0] & 0xff) << 24) |
            (((int)barr[1] & 0xff) << 16) |
            (((int)barr[2] & 0xff) << 8) |
            (((int)barr[3] & 0xff) << 0);

    // Do mask math in 64-bit to handle 32-bit wrapping cases.
    long mask1 = ((long)1 << (32 - _bits));
    long mask2 = mask1 - 1;
    long mask3 = ~mask2;
    int mask4 = (int) (mask3 & 0xffffffff);

    if ((i & mask4) == (j & mask4)) {
      return true;
    }

    return false;
  }

  public static ArrayList<UserSpecifiedNetwork> calcArrayList(String networkOpt) {
    ArrayList<UserSpecifiedNetwork> networkList = new ArrayList<UserSpecifiedNetwork>();

    if (networkOpt == null) return networkList;

    String[] networks;
    if (networkOpt.contains(",")) {
      networks = networkOpt.split(",");
    }
    else {
      networks = new String[1];
      networks[0] = networkOpt;
    }

    for (int j = 0; j < networks.length; j++) {
      String n = networks[j];
      Pattern p = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)/(\\d+)");
      Matcher m = p.matcher(n);
      boolean b = m.matches();
      if (! b) {
        Log.err("network invalid: " + n);
        return null;
      }
      assert (m.groupCount() == 5);
      int o1 = Integer.parseInt(m.group(1));
      int o2 = Integer.parseInt(m.group(2));
      int o3 = Integer.parseInt(m.group(3));
      int o4 = Integer.parseInt(m.group(4));
      int bits = Integer.parseInt(m.group(5));

      UserSpecifiedNetwork usn = new UserSpecifiedNetwork(o1, o2, o3, o4, bits);
      if (! usn.valid()) {
        Log.err("network invalid: " + n);
        return null;
      }

      networkList.add(usn);
    }

    return networkList;
  }
}
