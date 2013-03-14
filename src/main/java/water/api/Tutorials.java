package water.api;

/**
 * Summary page referencing all tutorials.
 *
 * @author michal
 */
public class Tutorials extends HTMLOnlyRequest {

  protected String build(Response response) {
    return "<div class='container'><div class='hero-unit' style='overflow: hidden'>"
    + "<style scoped='scoped'>"
    + "  .col { height: 330px; position: relative;}"
    + "  .col p { overflow: hidden; text-overflow: ellipsis; -o-text-overflow: ellipsis;}"
    + "  .col a { position: absolute; right: 40px; bottom: 10px; } "
    + "</style>"
    + "<h1>H<sub>2</sub>O Tutorials</h1>"
    + "<blockquote><small>A unique way to explore H<sub>2</sub>O</small></blockquote>"

    + "<div class='row'>"

    + "<div class='span3 col'>"
    + "  <h2 style='margin: 20px 0 0 0'>Random Forest</h2>"
    +   "<p>Random forest is a classical machine learning algorithm serving for data classification. Learn how to use it with H<sub>2</sub>O.</it></p>"
    +   "<a href='/TutorialRFIris.html' class='btn btn-primary btn-large'>Try it!</a>"
    + "</div>"

    + "<div class='span3 col'>"
    +   "<h2 style='margin: 20px 0 0 0'>GLM</h2>"
    +   "<p>Generalized linear model is a generalization of linear regresion. Experience its unique power on the top of H<sub>2</sub>O.</p>"
    +   "<a href='/TutorialGLMProstate.html' class='btn btn-primary btn-large'>Try it!</a>"
    + "</div>"

    + "<div class='span3 col'>"
    + "<h2 style='margin: 20px 0 0 0'>K-means</h2>"
    + "<p>Perform cluster analysis with H<sub>2</sub>O and K-means algorithm. H<sub>2</sub>O implements K-means||, a highly scalable version.</p>"
    +   "<a href='/TutorialKMeans.html' class='btn btn-primary btn-large'>Try it!</a>"
    + "</div>"

    + "</div>"
    + "</div>"
    + "</div>";
  }
}
