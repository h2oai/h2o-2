# Steam

*Steam* is H<sub>2</sub>O's web client.

## Setup

**Step 1** Download and install Node.js from [http://nodejs.org/download/](http://nodejs.org/download/).
As of Node.js version 0.6.3, npm is bundled and installed automatically with the environment.

**Step 2** Install [Slim](http://slim-lang.com/) and [SASS](http://sass-lang.com/).

*Note: You'll need [Ruby](https://www.ruby-lang.org/) installed. You can install it via [RVM](https://rvm.io/) using* `\curl -sSL https://get.rvm.io | bash -s stable`

Then, install these gems -

    sudo gem install slim
    sudo gem install sass

**Step 3** Setup local dependencies. Assuming you've already cloned the h2o git repo -

    cd h2o/client
    make setup

## Build

    cd h2o/client
    make

## Launch

Point your browser to [http://localhost:54321/steam.html](http://localhost:54321/steam.html)

## Make tasks

* `make` or `make build` Build and deploy
* `make check` Check prerequisites
* `make test`  Run all tests
* `make smoke` Run tests, but bail on first failure
* `make debug` Run tests in debug mode
* `make spec`  Generate test specs
* `make coverage` Report test coverage
* `make doc`  Generate documentation
* `make clean` Clean up built sources
* `make setup` Set up dev dependencies
* `make reset` Clean up dev dependencies
