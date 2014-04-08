# Steam

*Steam* is H<sub>2</sub>O's web client.

## Setup

**Step 1** Install Node and Bower

Note: You might need to `sudo` these commands.

    brew install node
    npm install -g bower

**Step 2** Setup local dependencies. Assuming you've already cloned the h2o git repo -

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
