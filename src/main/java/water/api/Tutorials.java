package water.api;
import water.AbstractBuildVersion;
import water.H2O;
/**
 * Summary page referencing all tutorials.
 *
 * @author michal
 */
public class Tutorials extends HTMLOnlyRequest {

  @Override protected String build(Response response) {
    AbstractBuildVersion abv = H2O.getBuildVersion();
    String branchName = abv.branchName();
    String buildNumber = abv.buildNumber();
    String documentationUrl = "http://s3.amazonaws.com/h2o-release/h2o/" + branchName + "/" + buildNumber + "/docs-website";
    String RPackageDocumentationUrl = documentationUrl + "/Ruser/top.html";
    return "<div class='container'><div class='hero-unit' style='overflow: hidden'>"
    + "<style scoped='scoped'>"
    + "  h2 { font-size:18px; }"
    + "  p { font-size:16px; }"
    + "</style>"
    + "<h1>Learn H<sub>2</sub>O at <a href='http://learn.h2o.ai'>learn.h2o.ai</a></h1>"
    + "<h1>Learn more <a href='https://leanpub.com/u/h2oai'>@leanpub</a></h1>"
    + "<blockquote><medium>A unique way to explore H<sub>2</sub>O</medium></blockquote>"

    + "</div>"
    + "<div class='row'>"

    + "<div class='span2 col'>"
    + "<h2>Use H<sub>2</sub>O from R</h2>"
    + "<div style='background-color:#006dcc;color:white;background-image:linear-gradient(to bottom,#08c,#04c);text-align:center;font-size:70px;font-weight:bold;height:100px;line-height:100px;border-radius:15px;max-width:110px;margin-bottom:5px'>R</div>"
    + "<p>H<sub>2</sub>O supports both R and R Studio.</p>"
    +   "<a href='" + RPackageDocumentationUrl + "' class='btn btn-primary'>Try it!</a>"
    + "</div>"

    + "<div class='span2 col'>"
    + "  <h2>Random Forest</h2>"
    +   "<p>Random Forest is a classical machine learning method for classification and regression. Learn how to use it with H<sub>2</sub>O for better predictions.</it></p>"
    +   "<a href='/TutorialRFIris.html' class='btn btn-primary'>Try it!</a>"
    + "</div>"

    + "<div class='span2 col'>"
    + "  <h2>GBM</h2>"
    +   "<p>GBM uses gradient boosted trees for classification and regression, and is one of the most powerful machine learning methods in H<sub>2</sub>O.</p>"
    +   "<a href='/TutorialGBM.html' class='btn btn-primary'>Try it!</a>"
    + "</div>"

    + "<div class='span2 col'>"
    +   "<h2>GLM</h2>"
    +   "<p>Generalized linear model is a generalization of linear regression. Experience its unique power and blazing speed on top of H<sub>2</sub>O.</p>"
    +   "<a href='/TutorialGLMProstate.html' class='btn btn-primary'>Try it!</a>"
    + "</div>"

    + "<div class='span2 col'>"
    + "<h2>K-Means</h2>"
    + "<p>Perform clustering analysis with H<sub>2</sub>O. K-means is a highly scalable clustering algorithm for unsupervised learning on big data.</p>"
    +   "<a href='/TutorialKMeans.html' class='btn btn-primary'>Try it!</a>"
    + "</div>"

    + "<div class='span2 col'>"
    + "<h2>Deep Learning</h2>"
    + "<p>H<sub>2</sub>O's distributed Deep Learning gives you the power of deep neural networks for highest accuracy for classification and regression.</p>"
    +   "<a href='/TutorialDeepLearning.html' class='btn btn-primary'>Try it!</a>"
    + "</div>"

    + "</div>"
    + "</div>";
  }
}
