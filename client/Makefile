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
	$(BUILD)/scripts/typecheck.global.js \
	$(BUILD)/scripts/hypergraph.global.js \
	$(BUILD)/scripts/geyser.global.js \
	$(BUILD)/scripts/main.js

BROWSER_SCRIPTS=\
	$(BUILD)/scripts/dom.global.js \
	$(BUILD)/scripts/geyser.global.js \
	$(BUILD)/scripts/knockout.js \
	$(BUILD)/scripts/application.js \
	$(BUILD)/scripts/application-context.js \
	$(BUILD)/scripts/router.js \
	$(BUILD)/scripts/routes.js \
	$(BUILD)/scripts/error-monitor.js \
	$(BUILD)/scripts/event-log.js \
	$(BUILD)/scripts/cache.js \
	$(BUILD)/scripts/localStorage.js \
	$(BUILD)/scripts/h2o-proxy.js \
	$(BUILD)/scripts/xhr.js \
	$(BUILD)/scripts/topic-list-view.js \
	$(BUILD)/scripts/frame-list-view.js \
	$(BUILD)/scripts/model-list-view.js \
	$(BUILD)/scripts/scoring-list-view.js \
	$(BUILD)/scripts/notification-list-view.js \
	$(BUILD)/scripts/frame-view.js \
	$(BUILD)/scripts/model-view.js \
	$(BUILD)/scripts/model-selection-view.js \
	$(BUILD)/scripts/scoring-view.js \
	$(BUILD)/scripts/notification-view.js \
	$(BUILD)/scripts/scoring-selection-view.js \
	$(BUILD)/scripts/main-view.js \
	$(BUILD)/scripts/steam.js

TEMPLATES=\
	src/templates/main-view.jade \
	src/templates/topic-list-view.jade \
	src/templates/frame-list-view.jade \
	src/templates/model-list-view.jade \
	src/templates/scoring-list-view.jade \
	src/templates/notification-list-view.jade \
	src/templates/empty-view.jade \
	src/templates/frame-view.jade \
	src/templates/model-view.jade \
	src/templates/model-selection-view.jade \
	src/templates/scoring-view.jade \
	src/templates/notification-view.jade \
	src/templates/scoring-selection-view.jade

STYLES=\
	src/styles/variables.styl \
	src/styles/mixins.styl \
	src/styles/fonts.styl \
	src/styles/bootstrap.styl \
	src/styles/global.styl \
	src/styles/main-view.styl \
	src/styles/topic-list-view.styl \
	src/styles/frame-list-view.styl \
	src/styles/model-list-view.styl \
	src/styles/scoring-list-view.styl \
	src/styles/notification-list-view.styl \
	src/styles/empty-view.styl \
	src/styles/frame-view.styl \
	src/styles/model-view.styl \
	src/styles/model-selection-view.styl \
	src/styles/scoring-view.styl \
	src/styles/notification-view.styl \
	src/styles/scoring-selection-view.styl

TESTS=\
	$(BUILD)/tests/prelude.tests.js \
	$(BUILD)/tests/typecheck.tests.js \
	$(BUILD)/tests/hypergraph.tests.js \
	$(BUILD)/tests/geyser.tests.js \
	$(BUILD)/tests/base.tests.js

LIB_JS=\
	$(LIB)/stacktrace/stacktrace.js \
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
	$(LIB)/fontawesome/css/font-awesome.css \
	$(LIB)/bootstrap/dist/css/bootstrap.css

LIB_TEST_JS=\
	$(LIB)/jquery/dist/jquery.js \
	$(LIB)/lodash/dist/lodash.js \
	$(LIB)/knockout/knockout.js \
	$(LIB)/chai/chai.js \
	$(MODULES)/mocha/mocha.js

LIB_TEST_CSS=\
	$(MODULES)/mocha/mocha.css


ifeq ($(shell which node),)
default:
	@echo Dependency node not found.  Skipping UI build...  Try: brew install node for MacOS or look at h2o/client/README.md for other platforms. 1>&2

else
  ifeq ($(shell which npm),)
default:
	@echo Dependency npm not found.  Skipping UI build...  Try: brew install node for MacOS or look at h2o/client/README.md for other platforms. 1>&2

  else
    ifeq ($(shell which bower),)
default:
	@echo Dependency bower not found.  Skipping UI build...  Try: npm install -g bower 1>&2

    else
default: build

    endif
  endif
endif


setup: check
	@echo Prerequisites OK. Installing dependencies...
	npm install
	bower --config.interactive=false install
	@echo Done!

really_build: \
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
	$(DEPLOY)/fonts/fontawesome-webfont.eot \
	$(DEPLOY)/fonts/fontawesome-webfont.svg \
	$(DEPLOY)/fonts/fontawesome-webfont.ttf \
	$(DEPLOY)/fonts/fontawesome-webfont.woff \
	$(DEPLOY)/img/h2o.png \
	$(DEPLOY)/css/bootstrap.css.map \
	$(DEPLOY)/css/lib.css \
	$(DEPLOY)/css/steam.css \
	$(DEPLOY)/js/lib.js \
	$(DEPLOY)/js/steam.js

build: setup
	$(MAKE) really_build

unit: \
	$(DEPLOY)/lib.test.css \
	$(DEPLOY)/lib.test.js \
	$(DEPLOY)/tests.html \
	$(DEPLOY)/steam.test.js

node: \
	$(DIST)/steam.node.js

watch:
	$(BIN)/watchy --watch src -- make build test

preload:
	Rscript src/util/h2o-R/preload.R

help:
	@echo "Please use \`make <target>' where <target> is one of -"
	@echo ""
	@echo "Setup tasks:"
	@echo "  make check      Check prerequisites"
	@echo "  make setup      Set up dev dependencies"
	@echo "  make reset      Clean up dev dependencies"
	@echo "  make preload    Preload a few frames and models into H2O (requires R)"
	@echo ""
	@echo "Development tasks:"
	@echo "  make build      Build and deploy to ../lib/resources/steam/"
	@echo "  make unit       Build browser test suite"
	@echo "  make test       Run all tests"
	@echo "  make            Run all tests"
	@echo "  make smoke      Run tests, but bail on first failure"
	@echo "  make report     Run all tests, verbose, with specs"
	@echo "  make debug      Run tests in debug mode"
	@echo "  make spec       Compile test specs"
	@echo "  make coverage   Compile test coverage"
	@echo "  make doc        Compile code documentation"
	@echo "  make clean      Clean up build directories"
	@echo "  make watch      Watch for changes and run `make build test`"

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

$(DEPLOY)/index.html: src/templates/index.jade $(TEMPLATES)
	@mkdir -p $(@D)
	$(BIN)/jade --pretty --out $(@D) $<

$(DEPLOY)/css/steam.css: src/styles/steam.styl $(STYLES)
	@mkdir -p $(@D)
	$(BIN)/stylus --use nib --out $(@D) $<

$(DEPLOY)/js/steam.js: $(SCRIPTS) $(BROWSER_SCRIPTS)
	@mkdir -p $(@D)
	@echo '"use strict";' > $@
	@echo '(function(){' >> $@
	cat $^ >> $@
	@echo '}).call(this);' >> $@

$(DEPLOY)/img/%.png: src/images/%.png
	@mkdir -p $(@D)
	cp $< $@

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
$(DEPLOY)/fonts/%.eot: $(LIB)/fontawesome/fonts/%.eot
	@mkdir -p $(@D)
	cp $< $@
$(DEPLOY)/fonts/%.svg: $(LIB)/fontawesome/fonts/%.svg
	@mkdir -p $(@D)
	cp $< $@
$(DEPLOY)/fonts/%.ttf: $(LIB)/fontawesome/fonts/%.ttf
	@mkdir -p $(@D)
	cp $< $@
$(DEPLOY)/fonts/%.woff: $(LIB)/fontawesome/fonts/%.woff
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

$(DEPLOY)/css/%.map: $(LIB)/bootstrap/dist/css/%.map
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
	

.PHONY: all build node unit smoke test debug report spec coverage doc clean setup reset watch help
