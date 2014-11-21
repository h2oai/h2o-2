package hex;

import hex.glm.GLM2;
import hex.glm.GLMModel;
import hex.glm.GLMParams;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.fvec.Frame;
import water.util.Log;

import java.util.Random;

public class GLMRandomTest extends TestUtil {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(JUnitRunnerDebug.NODES);
  }

  private static class GLM2Test extends GLM2 {
    public void invokeServe() {
      Response r = serve();
      if(r.error() != null)
        throw new IllegalArgumentException("Got error " + r.error());
      _fjtask.join();
    }

    public static void runFraction(float fraction) throws Throwable {
      long seed = new Random().nextLong();
      Log.info("GLMRadnomTest: seed = " + seed);
      Random rng = new Random(seed);
      int testcount = 0;
      int count = 0;
      int jobId = 0;

      final Key dest = Key.make("DEST");
      for(boolean offset:new boolean[]{true,false}){
        for(boolean intercept:new boolean[]{true,false}) {
          for (int rows : new int[]{
            10,
            100,
            //            1000,
          }) {
            for (int cols : new int[]{
              1,
              10,
              //              100,
            }) {
              for (float categorical_fraction : intercept ? new float[]{
                0,
                0.1f,
                1
              } : new float[]{0}) {
                for (int factors : new int[]{
                  2,
                  10,
                }) {
                  for (int response_factors : new int[]{
                    1, //regression
                    2, //binomial
                  }) {
                    for (boolean positive_response : new boolean[]{
                      true,
                      false
                    }) {


                      for (int max_iter : new int[]{
                        1,
                        10
                      }) {
                        for (boolean standardize : new boolean[]{
                          true,
                          false,
                        }) {
                          for (int n_folds : new int[]{
                            0,
                            3,
                          }) {
                            for (GLMParams.Family family : new GLMParams.Family[]{
                              GLMParams.Family.gamma,
                              GLMParams.Family.gaussian,
                              //                          GLMParams.Family.tweedie, tweedie is unstable now
                              GLMParams.Family.binomial,
                              GLMParams.Family.poisson,
                            }) {
                              if (response_factors != 2 && family == GLMParams.Family.binomial) continue;
                              if (!positive_response && family == GLMParams.Family.gamma) continue;
                              if (!positive_response && family == GLMParams.Family.tweedie) continue;

                              for (GLMParams.Link link : new GLMParams.Link[]{
                                GLMParams.Link.family_default,
//                                GLMParams.Link.identity,
//                                GLMParams.Link.inverse,
//                                GLMParams.Link.log,
//                                GLMParams.Link.logit,
//                                GLMParams.Link.tweedie,
                              }) {
                                switch (family) {
                                  case gaussian:
                                    if (link != GLMParams.Link.identity && link != GLMParams.Link.log && link != GLMParams.Link.inverse)
                                      continue;
                                    break;
                                  case binomial:
                                    if (link != GLMParams.Link.logit && link != GLMParams.Link.log)
                                      continue;
                                    break;
                                  case poisson:
                                    if (link != GLMParams.Link.log && link != GLMParams.Link.identity)
                                      continue;
                                    break;
                                  case gamma:
                                    if (link != GLMParams.Link.inverse && link != GLMParams.Link.log && link != GLMParams.Link.identity)
                                      continue;
                                    break;
                                  case tweedie:
                                    if (link != GLMParams.Link.tweedie)
                                      continue;
                                    break;
                                }
                                for (double tweedie_variance_power : new double[]{
                                  0
                                }) {
                                  for (double[] alpha : new double[][]{
                                    new double[]{1e-5},
                                    new double[]{1},
                                    new double[]{0, 0.5, 1},
                                  }) {
                                    for (double[] lambda : new double[][]{
                                      new double[]{1e-4},
                                      new double[]{1e-3, 1e-3},
                                    }) {
                                      for (double beta_epsilon : new double[]{
                                        0,
                                        1e-4,
                                      }) {
                                        for (boolean higher_accuracy : new boolean[]{
                                          true,
                                          false,
                                        }) {
                                          for (boolean use_all_factor_levels : new boolean[]{
                                            true,
                                            false,
                                          }) {
                                            for (boolean lambda_search : new boolean[]{
                                              true,
                                              false,
                                            }) {
                                              for (boolean strong_rules : new boolean[]{
                                                true,
                                                false,
                                              }) {
                                                for (int max_predictors : new int[]{
                                                  -1,
                                                  3,
                                                }) {
                                                  for (int nlambdas : new int[]{
                                                    1,
                                                    5,
                                                    50
                                                  }) {
                                                    for (double lambda_min_ratio : new double[]{
                                                      1e-2,
                                                      1e-4,
                                                    }) {
                                                      for (double prior : new double[]{
                                                        -1,
                                                        0.001
                                                      }) {
                                                        for (boolean variable_importances : new boolean[]{
                                                          true,
                                                          false,
                                                        }) {
                                                          count++;
                                                          if (fraction < rng.nextFloat()) continue;
                                                          CreateFrame cf = new CreateFrame();
                                                          cf.key = "random";
                                                          cf.rows = rows;
                                                          cf.cols = cols;
                                                          cf.categorical_fraction = categorical_fraction;
                                                          cf.integer_fraction = 1 - categorical_fraction;
                                                          cf.factors = factors;
                                                          cf.response_factors = response_factors;
                                                          cf.positive_response = positive_response;
                                                          cf.seed = seed;
                                                          cf.serve();
                                                          Frame frame = UKV.get(Key.make(cf.key));

                                                          Log.info("**************************)");
                                                          Log.info("Starting test #" + count);
                                                          Log.info("**************************)");
                                                          {
                                                            GLM2Test p = new GLM2Test();
                                                            p.job_key = Key.make("RandomGLM_" + jobId++);
                                                            p.destination_key = dest;
                                                            p.source = frame;
                                                            p.response = frame.vecs()[0]; //response is always the first column
                                                            p.max_iter = max_iter;
                                                            p.standardize = standardize;
                                                            p.n_folds = n_folds;
                                                            p.family = family;
                                                            p.link = link;
                                                            p.tweedie_variance_power = tweedie_variance_power;
                                                            p.alpha = alpha;
                                                            p.lambda = lambda;
                                                            p.beta_epsilon = beta_epsilon;
                                                            p.higher_accuracy = higher_accuracy;
                                                            p.use_all_factor_levels = use_all_factor_levels;
                                                            p.lambda_search = lambda_search;
                                                            p.strong_rules = strong_rules;
                                                            p.max_predictors = max_predictors;
                                                            p.nlambdas = nlambdas;
                                                            p.lambda_min_ratio = lambda_min_ratio;
                                                            p.prior = prior;
                                                            p.variable_importances = variable_importances;
                                                            p.MAX_ITERATIONS_PER_LAMBDA = 5;
                                                            p.intercept = intercept;
                                                            p.offset = (offset && frame.numCols() > 2)?frame.vec(1):null;
                                                            try {
                                                              p.invokeServe();
                                                              assert p._done;
                                                              if (p.alpha.length > 1)
                                                                new GLMGrid.DeleteGridTsk(null, p.destination_key).submitTask();
                                                              else
                                                                new GLMModel.DeleteModelTask(null, p.destination_key).submitTask();
                                                              System.out.println("TEST DONE");
                                                            } catch (DException.DistributedException dex) {
                                                              if (dex.getMessage().contains("IllegalArgument"))
                                                                Log.info("Skipping invalid combination of arguments.");
                                                              else throw new RuntimeException(dex);
                                                            } catch (IllegalArgumentException t) {
                                                              Log.info("Skipping invalid combination of arguments.");
                                                              // accept IllegalArgumentException, but nothing else
                                                            } finally {
                                                              frame.delete();
                                                            }
                                                          }
                                                          Log.info("Parameters combination " + count + ": PASS");
                                                          testcount++;
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        Log.info("\n\n=============================================");
        Log.info("Tested " + testcount + " out of " + count + " parameter combinations.");
        Log.info("=============================================");
      }
    }
  }

  public static class Long extends GLMRandomTest {
    @Test public void run() throws Throwable { GLM2Test.runFraction(0.1f); }
  }

  public static class Short extends GLMRandomTest {
    @Test public void run() throws Throwable { GLM2Test.runFraction(1e-6f); }
  }
}
