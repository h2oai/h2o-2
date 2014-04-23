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

Run `make help` to get a list of `make` tasks.

    $ make help

    Please use `make <target>' where <target> is one of -

  	Setup tasks:
  	  make check      Check prerequisites
  	  make setup      Set up dev dependencies
  	  make reset      Clean up dev dependencies
  	  make preload    Preload a few frames and models into H2O (requires R)
  	
  	Development tasks:
  	  make build      Build and deploy to ../lib/resources/steam/
  	  make unit       Build browser test suite
  	  make test       Run all tests
  	  make            Run all tests
  	  make smoke      Run tests, but bail on first failure
  	  make report     Run all tests, verbose, with specs
  	  make debug      Run tests in debug mode
  	  make spec       Compile test specs
  	  make coverage   Compile test coverage
  	  make doc        Compile code documentation
  	  make clean      Clean up build directories
  	  make watch      Watch for changes and run `make build test`
