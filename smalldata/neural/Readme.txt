Datasets Info -
(ftp://ftp.sas.com/pub/neural/FAQ3.html#A_hl)

1) Eight_sq.data(shuffled) (classification)
	There are two continuous inputs X and Y which take values uniformly distributed on a square [0,8] by [0,8]. Think of the input space as a chessboard, and number the squares 1 to 64. The categorical target variable C is the square number, so there are 64 output units. For example, you could generate the data as follows (this is the SAS programming language, but it should be easy to translate into any other language):

    data chess;
       step = 1/4;
       do x = step/2 to 8-step/2 by step;
          do y = step/2 to 8-step/2 by step;
             c = 8*floor(x) + floor(y) + 1;
             output;
          end;
       end;
    run;

    No hidden units are needed.

2) two_spiral.data (shuffled) (classification)
	The classic two-spirals problem has two continuous inputs and a Boolean classification target. The data can be generated as follows:

    data spirals;
       pi = arcos(-1);
       do i = 0 to 96;
          angle = i*pi/16.0;
          radius = 6.5*(104-i)/104;
          x = radius*cos(angle);
          y = radius*sin(angle);
          c = 1;
          output;
          x = -x;
          y = -y;
          c = 0;
          output;
       end;
    run;

    With one hidden layer, about 50 tanh hidden units are needed. Many NN programs may actually need closer to 100 hidden units to get zero training error.

3) sin_pattern.data(not shuffled) (regression)
	There is one continuous input X that takes values on [0,100]. There is one continuous target Y = sin(X). Getting a good approximation to Y requires about 20 to 25 tanh hidden units. Of course, 1 sine hidden unit would do the job. 


4) Benchmark_dojo_test.data (regression)
	The Donoho-Johnstone benchmarks consist of four functions (called Blocks, Bumps, Heavisine, and Doppler) to which random noise can be added to produce an infinite number of data sets. These benchmarks have one input, low noise, and high nonlinearity.
	This Dataset is from the noise-free functions, scaled to a standard deviation of 7
ftp://ftp.sas.com/pub/neural/dojo/dojo.html


