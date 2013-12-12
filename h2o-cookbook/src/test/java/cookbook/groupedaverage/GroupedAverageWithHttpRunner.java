package cookbook.groupedaverage;

import water.H2O;
import water.api.Request;
import water.api.RequestServer;
import water.util.Log;

public class GroupedAverageWithHttpRunner {
    public static void main(String[] args) throws Exception {
        water.Boot.main(UserMain.class, args);
    }

    public static class UserMain {
        public static void main(String[] args) {
            // Start H2O
            H2O.main(args);
            System.out.println("Starting...");
            registerCustomPages();
        }

        public static void registerCustomPages() {
            try {
                Request r = (Request)Class.forName("cookbook.groupedaverage.GroupedAverage").newInstance();
                Request.addToNavbar(RequestServer.registerRequest(r), "Grouped Average", "Grouped");
                Request.initializeNavBar();
                System.out.println("Pages added.");
            }
            catch (Exception e) {
                Log.err(e);
            }
        }
    }
}
