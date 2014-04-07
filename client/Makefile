BUILD=build
LIB=lib
MODULES=node_modules
BIN=$(MODULES)/.bin

all: build

check:
	@echo Checking prerequisites...

ifeq ($(shell which node),)
	$(error Dependency node not found. Exiting.)
else
	@echo node ... OK
endif

ifeq ($(shell which npm),)
	$(error Dependency npm not found. Exiting.)
else
	@echo npm ... OK
endif

build:

pack:\
	build \
	$(BUILD)/pack/steam.js

smoke: pack
	@$(BIN)/mocha \
		--reporter dot \
		--bail \
		--check-leaks \
		$(BUILD)/test/*.js

test: pack
	@$(BIN)/mocha \
		--reporter dot \
		--check-leaks \
		$(BUILD)/test/*.js

debug: pack
	@$(BIN)/mocha \
		--reporter dot \
		--check-leaks \
		--debug-brk \
		$(BUILD)/test/*.js

spec: pack
	@$(BIN)/mocha \
		--reporter spec \
		--check-leaks \
		$(BUILD)/test/*.js

coverage: pack
	$(BIN)/istanbul \
		cover \
		--dir build/coverage \
		$(BIN)/_mocha build/test/*.js 

doc: $(BUILD)/doc/index.html

clean:
	rm -rf \
		$(BUILD)/bin \
		$(BUILD)/test \
		$(BUILD)/coverage \
		$(BUILD)/doc \
		$(BUILD)/pack \
		$(BUILD)/steam.html
	@echo Cleaned!

setup: check
	@echo Prerequisites OK. Installing dependencies...
	npm install
	./node_modules/bower/bin/bower install
	@echo Done!

reset: clean
	rm -rf \
		$(MODULES) \
		$(LIB)
	@echo Done! To set up your dev environment, run: make setup

$(BUILD)/bin/%.js: src/components/%.coffee
	$(BIN)/coffee -o $(@D) -c $<

$(BUILD)/test/%.js: test/components/%.coffee
	$(BIN)/coffee -o $(@D) -c $<

$(BUILD)/bin/%.part.html: src/templates/%.jade
	$(BIN)/jade --pretty < $< > $@

$(BUILD)/bin/%.part.css: src/styles/%.styl
	$(BIN)/stylus < $< > $@

$(BUILD)/doc/index.html: pack doc/header.html doc/footer.html
	@$(BIN)/mocha \
		--reporter doc \
		$(BUILD)/test/*.js \
		| cat doc/header.html - doc/footer.html \
		> $@

.PHONY: all build pack smoke test debug spec coverage doc clean setup reset
