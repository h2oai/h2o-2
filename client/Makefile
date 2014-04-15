BUILD=build
DIST=$(BUILD)/dist
DEPLOY=../lib/resources/steam
LIB=lib
MODULES=node_modules
BIN=$(MODULES)/.bin

BUILD_DIRS=\
	$(BUILD)/scripts \
	$(BUILD)/tests \
	$(BUILD)/coverage \
	$(BUILD)/doc \
	$(BUILD)/spec \
	$(DIST) \
	$(DEPLOY)

SCRIPTS=\
	$(BUILD)/scripts/prelude.global.js \
	$(BUILD)/scripts/hypergraph.global.js \
	$(BUILD)/scripts/base.js

BOOT=\
	$(BUILD)/scripts/steam.js

TESTS=\
	$(BUILD)/tests/prelude.tests.js \
	$(BUILD)/tests/hypergraph.tests.js \
	$(BUILD)/tests/base.tests.js

LIB_JS=\
	$(LIB)/jquery/dist/jquery.js \
	$(LIB)/jquery-timeago/jquery.timeago.js \
	$(LIB)/lodash/dist/lodash.js \
	$(LIB)/js-signals/dist/signals.js \
	$(LIB)/crossroads/dist/crossroads.js \
	$(LIB)/hasher/dist/js/hasher.js \
	$(LIB)/bootstrap/dist/js/bootstrap.js \
	$(LIB)/d3/d3.js \
	$(LIB)/knockout/knockout.js

LIB_CSS=\
	$(LIB)/bootstrap/dist/css/bootstrap.css

LIB_TEST_JS=\
	$(LIB)/lodash/dist/lodash.js \
	$(LIB)/knockout/knockout.js \
	$(LIB)/chai/chai.js \
	$(MODULES)/mocha/mocha.js

LIB_TEST_CSS=\
	$(MODULES)/mocha/mocha.css

all: build

build: \
	$(DEPLOY)/index.html \
	$(DEPLOY)/fonts/glyphicons-halflings-regular.eot \
	$(DEPLOY)/fonts/glyphicons-halflings-regular.svg \
	$(DEPLOY)/fonts/glyphicons-halflings-regular.ttf \
	$(DEPLOY)/fonts/glyphicons-halflings-regular.woff \
	$(DEPLOY)/fonts/Roboto-Bold-webfont.eot \
	$(DEPLOY)/fonts/Roboto-Bold-webfont.svg \
	$(DEPLOY)/fonts/Roboto-Bold-webfont.ttf \
	$(DEPLOY)/fonts/Roboto-Bold-webfont.woff \
	$(DEPLOY)/fonts/Roboto-Light-webfont.eot \
	$(DEPLOY)/fonts/Roboto-Light-webfont.svg \
	$(DEPLOY)/fonts/Roboto-Light-webfont.ttf \
	$(DEPLOY)/fonts/Roboto-Light-webfont.woff \
	$(DEPLOY)/fonts/Roboto-Regular-webfont.eot \
	$(DEPLOY)/fonts/Roboto-Regular-webfont.svg \
	$(DEPLOY)/fonts/Roboto-Regular-webfont.ttf \
	$(DEPLOY)/fonts/Roboto-Regular-webfont.woff \
	$(DEPLOY)/css/lib.css \
	$(DEPLOY)/css/steam.css \
	$(DEPLOY)/js/lib.js \
	$(DEPLOY)/js/steam.js

unit: \
	$(DEPLOY)/lib.test.css \
	$(DEPLOY)/lib.test.js \
	$(DEPLOY)/tests.html \
	$(DEPLOY)/steam.test.js

node: \
	$(DIST)/steam.node.js

help:
	@echo "Please use \`make <target>' where <target> is one of -"
	@echo ""
	@echo "Setup tasks:"
	@echo "  make check      Check prerequisites"
	@echo "  make setup      Set up dev dependencies"
	@echo "  make reset      Clean up dev dependencies"
	@echo ""
	@echo "Development tasks:"
	@echo "  make build      Build and deploy to ../lib/resources/steam/"
	@echo "  make unit       Build browser test suite"
	@echo "  make test       Run all tests"
	@echo "  make smoke      Run tests, but bail on first failure"
	@echo "  make report     Run all tests, verbose, with specs"
	@echo "  make debug      Run tests in debug mode"
	@echo "  make spec       Compile test specs"
	@echo "  make coverage   Compile test coverage"
	@echo "  make doc        Compile code documentation"
	@echo "  make clean      Clean up build directories"

smoke: node
	$(BIN)/mocha \
		--reporter dot \
		--bail \
		--check-leaks \
		$(DIST)/steam.node.js

test: node
	$(BIN)/mocha \
		--reporter dot \
		--check-leaks \
		$(DIST)/steam.node.js

debug: node
	$(BIN)/mocha \
		--reporter dot \
		--check-leaks \
		--debug-brk \
		$(DIST)/steam.node.js

report: node
	$(BIN)/mocha \
		--reporter spec \
		--check-leaks \
		$(DIST)/steam.node.js

coverage: node
	@mkdir -p $(BUILD)/coverage
	$(BIN)/istanbul \
		cover \
		$(BIN)/_mocha \
		--dir $(BUILD)/coverage \
		-x "**/lib/**" \
		$(DIST)/steam.node.js
	@echo Coverage generated at $(BUILD)/coverage

spec: $(BUILD)/spec/index.html

doc: 
	@mkdir -p $(BUILD)/doc
	$(BIN)/groc \
		--out $(BUILD)/doc \
		--index src/README.md \
		--index-page-title "index" \
		src/scripts/**/*.coffee
	@echo Docs generated at $(BUILD)/doc

clean:
	rm -rf $(BUILD_DIRS)
	@echo Cleaned!

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

setup: check
	@echo Prerequisites OK. Installing dependencies...
	npm install
	bower --config.interactive=false install
	@echo Done!

reset: clean
	rm -rf \
		$(MODULES) \
		$(LIB)
	@echo Done! To set up your dev environment again, run: make setup

$(BUILD)/scripts/%.global.js: src/scripts/%.global.coffee
	@mkdir -p $(@D)
	$(BIN)/coffee --bare --output $(@D) --compile $<

$(BUILD)/tests/%.global.js: src/tests/%.global.coffee
	@mkdir -p $(@D)
	$(BIN)/coffee --bare --output $(@D) --compile $<

$(BUILD)/scripts/%.js: src/scripts/%.coffee
	@mkdir -p $(@D)
	$(BIN)/coffee --output $(@D) --compile $<

$(BUILD)/tests/%.js: src/tests/%.coffee
	@mkdir -p $(@D)
	$(BIN)/coffee --output $(@D) --compile $<

$(DEPLOY)/index.html: src/templates/index.jade
	@mkdir -p $(@D)
	$(BIN)/jade --pretty --out $(@D) $<

$(DEPLOY)/css/steam.css: src/styles/steam.styl
	@mkdir -p $(@D)
	$(BIN)/stylus --out $(@D) $<

$(DEPLOY)/js/steam.js: $(SCRIPTS) $(BOOT)
	@mkdir -p $(@D)
	@echo '"use strict";' > $@
	@echo '(function(){' >> $@
	cat $^ >> $@
	@echo '}).call(this);' >> $@


$(DEPLOY)/fonts/%.eot: src/fonts/%.eot
	@mkdir -p $(@D)
	cp $< $@
$(DEPLOY)/fonts/%.svg: src/fonts/%.svg
	@mkdir -p $(@D)
	cp $< $@
$(DEPLOY)/fonts/%.ttf: src/fonts/%.ttf
	@mkdir -p $(@D)
	cp $< $@
$(DEPLOY)/fonts/%.woff: src/fonts/%.woff
	@mkdir -p $(@D)
	cp $< $@
$(DEPLOY)/fonts/%.eot: $(LIB)/bootstrap/dist/fonts/%.eot
	@mkdir -p $(@D)
	cp $< $@
$(DEPLOY)/fonts/%.svg: $(LIB)/bootstrap/dist/fonts/%.svg
	@mkdir -p $(@D)
	cp $< $@
$(DEPLOY)/fonts/%.ttf: $(LIB)/bootstrap/dist/fonts/%.ttf
	@mkdir -p $(@D)
	cp $< $@
$(DEPLOY)/fonts/%.woff: $(LIB)/bootstrap/dist/fonts/%.woff
	@mkdir -p $(@D)
	cp $< $@

$(DIST)/steam.node.js: $(BUILD)/tests/node.global.js $(SCRIPTS) $(TESTS)
	@mkdir -p $(@D)
	@echo '"use strict";' > $@
	@echo '(function(){' >> $@
	cat $^ >> $@
	@echo '}).call(this);' >> $@

$(DEPLOY)/steam.test.js: $(BUILD)/tests/browser.global.js $(SCRIPTS) $(TESTS)
	@mkdir -p $(@D)
	@echo '"use strict";' > $@
	@echo '(function(){' >> $@
	cat $^ >> $@
	@echo '}).call(this);' >> $@

$(DEPLOY)/tests.html: src/util/mocha-html/tests.html
	@mkdir -p $(@D)
	cp $< $@

$(DEPLOY)/css/lib.css: $(LIB_CSS)
	@mkdir -p $(@D)
	cat $^ > $@

$(DEPLOY)/js/lib.js: $(LIB_JS)
	@mkdir -p $(@D)
	cat $^ > $@

$(DEPLOY)/lib.test.css: $(LIB_TEST_CSS)
	@mkdir -p $(@D)
	cat $^ > $@

$(DEPLOY)/lib.test.js: $(LIB_TEST_JS)
	@mkdir -p $(@D)
	cat $^ > $@

$(BUILD)/spec/index.html: node
	@mkdir -p $(@D)
	@$(BIN)/mocha \
		--reporter doc \
		$(DIST)/steam.node.js \
		| cat src/util/mocha-doc/header.html - src/util/mocha-doc/footer.html \
		> $@
	@echo Spec generated at $(@D)
	

.PHONY: all build node unit smoke test debug report spec coverage doc clean setup reset help
