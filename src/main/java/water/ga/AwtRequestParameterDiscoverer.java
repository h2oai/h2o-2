package water.ga;

import static water.ga.GaUtils.isEmpty;

import java.awt.Dimension;
import java.awt.GraphicsDevice;
import java.awt.GraphicsEnvironment;
import java.awt.Toolkit;

/**
 * Clases uses AWT classes to discover following properties.
 * <ul>
 * 	<li>Screen Resolution</li>
 *  <li>Screen Colors</li>
 * </ul>
 *
 * @author Santhosh Kumar
 *
 * This copy of google-analytics-java is a back port of version 1.1.1 of the library.
 * This backport removes the slf4j dependency, and modifies the code to work with the
 * 4.1 version of the Apache http client library.
 *
 * Original sources can be found at https://github.com/brsanthu/google-analytics-java.
 * All copyrights retained by original authors.
 */
public class AwtRequestParameterDiscoverer extends DefaultRequestParameterDiscoverer {

  @Override
  public DefaultRequest discoverParameters(GoogleAnalyticsConfig config, DefaultRequest request) {
    super.discoverParameters(config, request);

    Toolkit toolkit = Toolkit.getDefaultToolkit();

    if (isEmpty(request.screenResolution())) {
      Dimension screenSize = toolkit.getScreenSize();
      request.screenResolution(((int) screenSize.getWidth()) + "x" + ((int) screenSize.getHeight()) + ", " + toolkit.getScreenResolution() + " dpi");
    }

    if (isEmpty(request.screenColors())) {
      GraphicsEnvironment graphicsEnvironment = GraphicsEnvironment.getLocalGraphicsEnvironment();
      GraphicsDevice[] graphicsDevices = graphicsEnvironment.getScreenDevices();

      StringBuilder sb = new StringBuilder();
      for (GraphicsDevice graphicsDevice : graphicsDevices) {
        if (sb.length() != 0) {
          sb.append(", ");
        }
        sb.append(graphicsDevice.getDisplayMode().getBitDepth());
      }
      request.screenColors(sb.toString());
    }

    return request;
  }
}
