# 
# Simple SBT wrapper for building and cleaning the project
#

# Setup sbt location
TRUE=$(shell which true)
SBT=$(shell which sbt || which true)
# Suppress printing commands
D=@

all: clean build

init:
ifeq ($(SBT),$(TRUE))
	$(D) echo "!!! SBT is not installed ! Please follow instructions here: http://www.scala-sbt.org/release/docs/Getting-Started/Setup"
endif

build: init
	$(D) "$(SBT)" package

clean: init
	$(D) "$(SBT)" clean

