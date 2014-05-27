(function() {
  if (!Array.prototype.filter) {
    Array.prototype.filter = function(callback) {
      var element, _i, _len, _results;
      _results = [];
      for (_i = 0, _len = this.length; _i < _len; _i++) {
        element = this[_i];
        if (callback(element)) {
          _results.push(element);
        }
      }
      return _results;
    };
  }

  if (!Array.prototype.sum) {
    Array.prototype.sum = function() {
      var i, result, _i, _ref;
      result = 0;
      for (i = _i = 0, _ref = this.length; 0 <= _ref ? _i < _ref : _i > _ref; i = 0 <= _ref ? ++_i : --_i) {
        result += this[i];
      }
      return result;
    };
  }

  if (!Array.prototype.swap) {
    Array.prototype.swap = function(idxA, idxB) {
      var itemA;
      itemA = this[idxA];
      this[idxA] = this[idxB];
      return this[idxB] = itemA;
    };
  }

  if (!Array.prototype.move) {
    Array.prototype.move = function(fromIdx, toIdx) {
      if (this.splice != null) {
        return this.splice(toIdx, 0, this.splice(fromIdx, 1)[0]);
      }
    };
  }

}).call(this);

(function() {
  this.uuid = function() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      var r, v;
      r = Math.random() * 16 | 0;
      v = c === 'x' ? r : r & 0x3 | 0x8;
      return v.toString(16);
    });
  };

}).call(this);

(function() {
  $.fn.isOnScreen = function(scrollTop, scrollLeft, windowWidth, windowHeight) {
    var boundsBottom, boundsLeft, boundsRight, boundsTop, isVisible, scrollBottom, scrollRight;
    scrollRight = scrollLeft + windowWidth;
    scrollBottom = scrollTop + windowHeight;
    boundsLeft = this[0].offsetLeft;
    boundsTop = this[0].offsetTop;
    if (scrollRight < boundsLeft || scrollBottom < boundsTop) {
      return false;
    }
    isVisible = this.is(':visible');
    if (isVisible) {
      boundsRight = boundsLeft + this[0].offsetWidth;
    } else {
      boundsRight = boundsLeft;
    }
    if (scrollLeft > boundsRight) {
      return false;
    }
    if (isVisible) {
      boundsBottom = boundsTop + this[0].offsetHeight;
    } else {
      boundsBottom = boundsTop;
    }
    if (scrollTop > boundsBottom) {
      return false;
    }
    return true;
  };

}).call(this);

(function() {
  $.widget("swing.reorderabletable", {
    eventWidgetPrefix: 'dragtable',
    options: {
      draggedElementClass: 'dragged-header-table',
      newColumnSpotPlaceholderClass: 'dragged-header-new-column-spot-placeholder',
      dataHeader: 'data-header',
      appendTargetSelector: "body",
      headerSelector: "th:not(.no-drag)",
      scroll: true,
      stop: function() {}
    },
    _create: function() {
      this._mouseDownHandler = this._mouseDownHandlerFactory(this);
      this._mouseMoveHandler = this._mouseMoveHandlerFactory(this);
      this._mouseUpHandler = this._mouseUpHandlerFactory(this);
      return this.element.on('mousedown', this.options.headerSelector, this._mouseDownHandlerFactory(this));
    },
    _destroy: function() {
      this.destroyDraggedElement();
      return this.element.off('mousedown', this.options.headerSelector);
    },
    $columnHeaders: function() {
      return $(this.element.find(this.options.headerSelector));
    },
    recreateColumnSpotsCache: function() {
      return this.columnPositions = this.$columnHeaders().map(function(col) {
        return $(this).offset().left;
      });
    },
    columnIndexUnderDraggedElement: function() {
      var draggedElementPosX, idx, leftPos, _i, _len, _ref;
      draggedElementPosX = this.$draggedElement.offset().left + 0.5 * this.$draggedElement.outerWidth();
      idx = -1;
      _ref = this.columnPositions;
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        leftPos = _ref[_i];
        if (leftPos > draggedElementPosX) {
          return idx;
        }
        idx += 1;
      }
      return idx;
    },
    createDraggedElementFrom: function($th, e) {
      this.destroyDraggedElement();
      this.recreateColumnSpotsCache();
      this.$draggedElement = $("<table></table>");
      this.$draggedElementTR = $("<tr></tr>");
      this.$draggedElementTH = $th.clone();
      this.$draggedElementTH.css({
        width: $th.width() - 1,
        height: $th.height() - 1
      });
      this.$draggedElement.append(this.$draggedElementTR);
      this.$draggedElementTR.append(this.$draggedElementTH);
      this.$draggedElement.attr('class', this.element.attr('class'));
      if (this.options.draggedElementClass != null) {
        this.$draggedElement.addClass(this.options.draggedElementClass);
      }
      this.startDragFrom($th, e);
      this.updateCursorPositionFromEvent(e);
      this.updateDraggedElementPosition();
      $(this.options.appendTargetSelector).append(this.$draggedElement);
      $(document.body).bind('mouseup', this._mouseUpHandler);
      return $(document.body).bind("mousemove", this._mouseMoveHandler);
    },
    destroyDraggedElement: function() {
      this.dehighlightPlaceOfInsertion();
      if (!this.$draggedElement) {
        return;
      }
      this.$draggedElement.detach();
      $(document.body).unbind("mousemove", this._mouseMoveHandler);
      return $(document.body).unbind('mouseup');
    },
    highlightPlaceOfInsertion: function() {
      var $columnUnderCursorElement;
      if (this.$highlightElement == null) {
        this.$highlightElement = $("<div></div>");
        this.$highlightElement.addClass(this.options.newColumnSpotPlaceholderClass);
        this.$highlightElement.css({
          position: "absolute"
        });
        $(this.options.appendTargetSelector).append(this.$highlightElement);
      }
      $columnUnderCursorElement = $(this.$columnHeaders()[this.currentlyOverIndex]);
      this.$highlightElement.css({
        left: this.columnPositions[this.currentlyOverIndex],
        top: this.element.offset().top,
        width: $columnUnderCursorElement.outerWidth(),
        height: this.element.height()
      });
    },
    dehighlightPlaceOfInsertion: function() {
      if (!this.$highlightElement) {
        return;
      }
      this.$highlightElement.detach();
      return this.$highlightElement = null;
    },
    startDragFrom: function($th, e) {
      this.origin = $th.offset();
      return this.originCursorX = e.pageX;
    },
    updateCursorPositionFromEvent: function(e) {
      return this.currentCursorX = e.pageX;
    },
    updateDraggedElementPosition: function() {
      return this.$draggedElement.css({
        position: "absolute",
        left: this.origin.left + (this.currentCursorX - this.originCursorX) - 1,
        top: this.origin.top
      });
    },
    order: function() {
      var columnIDAttr, order, swapItemIdx, withItemIdx;
      columnIDAttr = this.options.dataHeader;
      order = $.map(this.$columnHeaders(), function(item, idx) {
        return $(item).attr(columnIDAttr);
      });
      if (this.originalIndex !== this.currentlyOverIndex) {
        swapItemIdx = this.originalIndex;
        withItemIdx = this.currentlyOverIndex < 0 ? 0 : this.currentlyOverIndex;
        order.move(swapItemIdx, withItemIdx);
      }
      return order;
    },
    _mouseDownHandlerFactory: function(self) {
      return function(e) {
        self.createDraggedElementFrom($(this), e);
        self.originalIndex = self.$columnHeaders().index(this);
        self.currentlyOverIndex = self.originalIndex;
        return false;
      };
    },
    _mouseUpHandlerFactory: function(self) {
      return function(e) {
        self.destroyDraggedElement();
        return self.options.stop(self.order());
      };
    },
    _mouseMoveHandlerFactory: function(self) {
      return function(e) {
        self.updateCursorPositionFromEvent(e);
        self.updateDraggedElementPosition();
        self.currentlyOverIndex = self.columnIndexUnderDraggedElement();
        return self.highlightPlaceOfInsertion();
      };
    }
  });

}).call(this);

(function() {
  var h2oInspect, h2oParse;

  angular.module('h2o', ['h2o.directives', 'swing.filters', 'h2o.services']);

  h2oInspect = angular.module('h2o.inspect', ['h2o.directives', 'h2o.directives.inspect', 'swing.directives', 'swing.directives.inspect', 'swing.filters', 'h2o.services', 'h2o.services.typeahead', 'h2o.services.inspect', 'h2o.controllers.inspect']);

  h2oParse = angular.module('h2o.parse', ['h2o.directives', 'h2o.directives.parse', 'swing.directives', 'swing.directives.parse', 'swing.filters', 'h2o.services', 'h2o.services.typeahead', 'h2o.services.parse', 'h2o.controllers.parse', 'ui.router', 'angularFileUpload', 'ui.utils']);

  h2oParse.config(function($stateProvider, $urlRouterProvider) {
    $urlRouterProvider.otherwise("/choose_data_source/");
    $stateProvider.state('choose_data_source', {
      url: '/choose_data_source/{wizardID}',
      templateUrl: 'partials/Parse_Choose_data_source.html',
      controller: 'ChooseDataSourceController'
    }).state('filter_files', {
      url: '/filter_files/{wizardID}',
      templateUrl: 'partials/Parse_Filter_files.html',
      controller: 'FilterFilesController'
    }).state('preview', {
      url: '/preview/{wizardID}',
      templateUrl: 'partials/Parse_Preview.html',
      controller: 'PreviewController'
    }).state('progress', {
      url: '/progress/{wizardID}',
      templateUrl: 'partials/Parse_Progress.html',
      controller: 'ProgressController'
    });
  });

  if (ENV === "RELEASE") {
    h2oParse.config(function($logProvider) {
      return $logProvider.debugEnabled(false);
    });
    h2oInspect.config(function($logProvider) {
      return $logProvider.debugEnabled(false);
    });
  }

  angular.module('h2o.directives', ['swing.fastNg']);

  angular.module('h2o.directives.parse', ['h2o.services.parse']);

  angular.module('h2o.directives.inspect', ['h2o.services.inspect']);

  angular.module('swing.directives.errorAndStatus', []);

  angular.module('swing.directives.table', []);

  angular.module('swing.directives.textfield', []);

  angular.module('swing.directives.inspect', []);

  angular.module('swing.directives.parse', []);

  angular.module('swing.directives', ['swing.directives.table', 'swing.directives.textfield', 'swing.directives.errorAndStatus', 'swing.ngModelOnBlur']);

  angular.module('swing.filters', []);

  angular.module('swing.services.errorAndStatus', []);

  angular.module('swing.services', ['swing.services.errorAndStatus']);

  angular.module('h2o.services', ['swing.services']);

  angular.module('h2o.services.typeahead', ['siyfion.sfTypeahead']);

  angular.module('h2o.services.parse.common', []);

  angular.module('h2o.services.parse', ['h2o.services', 'h2o.services.parse.common']);

  angular.module('h2o.services.inspect', []);

  angular.module('h2o.controllers.parse', ['h2o.services', 'h2o.services.parse', 'localytics.directives', 'ngConfirmedClick', 'ngDragDrop', 'sly']);

  angular.module('h2o.controllers.inspect', ['h2o.services', 'h2o.services.inspect', 'ngConfirmedClick', 'sly']);

}).call(this);

(function() {
  var Model;

  Model = (function() {
    function Model(data) {
      if (data == null) {
        data = {};
      }
      this.update(data);
    }

    Model.prototype.update = function(data) {};

    Model.sanitizeString = function(input, defaultTo) {
      var value;
      if (defaultTo == null) {
        defaultTo = "";
      }
      if (input == null) {
        return defaultTo;
      }
      value = input + "";
      return value;
    };

    Model.sanitizeInt = function(input, defaultTo) {
      var value;
      if (defaultTo == null) {
        defaultTo = 0;
      }
      if (input == null) {
        return defaultTo;
      }
      if (input.toFixed) {
        return Math.round(input);
      }
      value = parseFloat(input, 10);
      if (isNaN(value)) {
        if (input === true) {
          return 1;
        } else if (input === false) {
          return 0;
        } else {
          return defaultTo;
        }
      }
      return Math.round(value);
    };

    Model.sanitizeArray = function(input, itemsClass) {
      var item;
      if (input instanceof Array) {
        if (itemsClass != null) {
          return (function() {
            var _i, _len, _results;
            _results = [];
            for (_i = 0, _len = input.length; _i < _len; _i++) {
              item = input[_i];
              _results.push(new itemsClass(item));
            }
            return _results;
          })();
        } else {
          return input;
        }
      } else {
        return [];
      }
    };

    Model.sanitizeFloat = function(input, defaultTo) {
      var value;
      if (defaultTo == null) {
        defaultTo = 0.0;
      }
      if (input == null) {
        return defaultTo;
      }
      if (input.toFixed) {
        return input;
      }
      value = parseFloat(input, 10);
      if (isNaN(value)) {
        if (input === true) {
          return 1.0;
        } else if (input === false) {
          return 0.0;
        } else {
          return defaultTo;
        }
      }
      return value;
    };

    Model.sanitizeBool = function(input, defaultTo) {
      var value;
      if (defaultTo == null) {
        defaultTo = false;
      }
      if (input == null) {
        return defaultTo;
      }
      value = input;
      if (value === false) {
        return false;
      }
      if (value === true) {
        return true;
      }
      if (value) {
        return true;
      }
      return false;
    };

    Model.sanitizeStringOrInt = function(input, defaultTo) {
      var intResult;
      if (defaultTo == null) {
        defaultTo = 0;
      }
      if (input == null) {
        return defaultTo;
      }
      intResult = this.sanitizeInt(input, defaultTo = null);
      if (!input.toFixed && !angular.equals(intResult + "", input)) {
        return this.sanitizeString(input);
      }
      return intResult;
    };

    Model.sanitizeStringOrFloat = function(input, defaultTo) {
      var floatResult;
      if (defaultTo == null) {
        defaultTo = 0.0;
      }
      if (input == null) {
        return defaultTo;
      }
      floatResult = this.sanitizeFloat(input, defaultTo = null);
      if (!input.toFixed && !angular.equals(floatResult + "", input)) {
        return this.sanitizeString(input);
      }
      return floatResult;
    };

    return Model;

  })();

  window.Model = Model;

}).call(this);

/*** @module h2o.controllers.inspect **/


(function() {
  var module;

  module = angular.module('h2o.controllers.inspect');

  /**
   * @namespace InspectAppController
   *
   * Main controller for the Inspect page.
   *
  */


  module.controller('InspectAppController', function($scope, InspectDataService, InspectColumnService, MenuService, TypeaheadService) {
    var _this = this;
    $scope.InspectDataService = InspectDataService;
    $scope.InspectColumnService = InspectColumnService;
    $scope.sortedBy = 'default';
    $scope.MenuService = MenuService;
    $scope.TypeaheadService = TypeaheadService;
    this.keyInput = "";
    this.init = function() {
      InspectDataService.fetch();
      return _this;
    };
    /**
     * Returns true if the current URI contains the 'key' param.
     *
     * @function $scope.isKeySetInURI
     * @memberOf InspectAppController
     *
    */

    $scope.isKeySetInURI = function() {
      var queryDict;
      queryDict = JSONApiServerURI().query(true);
      return (queryDict != null) && (queryDict.key != null);
    };
    return this.init();
  });

  /**
   * @namespace InspectColumnController
   *
   * Controls columns show/hide behaviour and ordering.
   *
  */


  module.controller('InspectColumnController', function($scope, $log, InspectDataService, InspectColumnService) {
    var _this = this;
    $scope.InspectDataService = InspectDataService;
    $scope.InspectColumnService = InspectColumnService;
    this.init = function() {};
    /**
     * Reorders columns using a function converting column dict to value they are to be reordered by.
     *
     * @param {Function} valueFunc
     * @param {Boolean} [ascending=true]
     *
     * @function $scope.sortByFunc
     * @memberOf InspectColumnController
     *
    */

    $scope.sortByFunc = function(valueFunc, ascending) {
      var c, headers, sortFunc;
      if (ascending == null) {
        ascending = true;
      }
      headers = new Array();
      angular.forEach(InspectDataService.columns, function(c) {
        return headers.push({
          name: c.name,
          value: valueFunc(c)
        });
      });
      if (ascending) {
        sortFunc = function(a, b) {
          return a.value - b.value;
        };
      } else {
        sortFunc = function(a, b) {
          return b.value - a.value;
        };
      }
      headers.sort(sortFunc);
      return InspectColumnService.setNewColumnOrderByNames((function() {
        var _i, _len, _results;
        _results = [];
        for (_i = 0, _len = headers.length; _i < _len; _i++) {
          c = headers[_i];
          _results.push(c.name);
        }
        return _results;
      })());
    };
    $scope.sortByDefault = function() {
      $scope.sortedBy = 'default';
      return InspectColumnService.revertToDefaultOrder();
    };
    $scope.sortByVariance = function() {
      $scope.sortedBy = 'variance';
      return $scope.sortByFunc((function(c) {
        return c.variance;
      }), false);
    };
    $scope.isSortedByDefault = function() {
      if ($scope.sortedBy === 'default') {
        return true;
      } else {
        return false;
      }
    };
    $scope.isSortedByVariance = function() {
      if ($scope.sortedBy === 'variance') {
        return true;
      } else {
        return false;
      }
    };
    $scope.showAllColumns = function() {
      var c, pattern, _i, _len, _ref;
      pattern = new RegExp($scope.columnFilter, "im");
      _ref = InspectColumnService.filterableTableHeaders;
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        c = _ref[_i];
        if (pattern.test(c['visual'])) {
          InspectColumnService.shownTableHeaders[c.name] = true;
        }
      }
      return InspectColumnService.refilter();
    };
    $scope.hideAllColumns = function() {
      var c, pattern, _i, _len, _ref;
      pattern = new RegExp($scope.columnFilter, "im");
      _ref = InspectColumnService.filterableTableHeaders;
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        c = _ref[_i];
        if (pattern.test(c['visual'])) {
          InspectColumnService.shownTableHeaders[c.name] = false;
        }
      }
      return InspectColumnService.refilter();
    };
    /**
     * Hides all columns containing NA values.
     *
     * @function $scope.hideMissingColumns
     * @memberOf InspectColumnController
     *
    */

    $scope.hideMissingColumns = function() {
      var c, noValues, _i, _len, _ref;
      _ref = InspectColumnService.filterableTableHeaders;
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        c = _ref[_i];
        noValues = c.num_missing_values === InspectDataService.numRows;
        InspectColumnService.shownTableHeaders[c.name] = !noValues;
      }
      return InspectColumnService.refilter();
    };
    return this.init();
  });

  /**
   * @namespace InspectTableController
   *
   * Inspect table controller.
   *
  */


  module.controller('InspectTableController', function($scope, $log, InspectDataService, InspectColumnService) {
    var _this = this;
    $scope.InspectDataService = InspectDataService;
    $scope.InspectColumnService = InspectColumnService;
    $scope.tableHeaders = [];
    $scope.tableData = [];
    $scope.isMoreNasToRight = function() {
      return false;
    };
    $scope.isMoreNasToLeft = function() {
      return false;
    };
    this.init = function() {
      $scope.$watch('InspectColumnService.filteredTableHeaders', function(newVal, oldVal, scope) {
        return $scope.tableHeaders = newVal != null ? newVal : [];
      });
      $scope.$watch('InspectColumnService.tableData', function(newVal, oldVal, scope) {
        return $scope.tableData = newVal != null ? newVal : [];
      });
    };
    /**
     * Reorders columns.
     * @param {Array} newNamesOrder - array of column names – columns will be rearranged in this order.
     *
     * @function $scope.reorder
     * @memberOf InspectTableController
     *
    */

    $scope.reorder = function(newNamesOrder) {
      newNamesOrder.unshift("row");
      return InspectColumnService.setNewColumnOrderByNames(newNamesOrder);
    };
    return this.init();
  });

  /**
   * @namespace InspectPaginationController
   *
   * Controls Table row pagination.
   *
  */


  module.controller('InspectPaginationController', function($scope, $log, InspectDataService) {
    var _this = this;
    $scope.InspectDataService = InspectDataService;
    $scope.offset = 0;
    $scope.limit = 0;
    $scope.firstRow = 0;
    $scope.lastRow = 0;
    $scope.pageOffset = 0;
    $scope.firstPage = 0;
    $scope.lastPage = 0;
    $scope.canGoToNextPage = false;
    $scope.canGoToPrevPage = false;
    $scope.isLoading = false;
    this.init = function() {
      var _this = this;
      $scope.$watch('InspectDataService.numRows', function(newVal, oldVal, scope) {
        if (newVal) {
          $scope.lastRow = newVal;
        }
        return _this.recalculatePages();
      });
      $scope.$watch('InspectDataService.offset', function(newVal, oldVal, scope) {
        $scope.offset = newVal;
        return _this.recalculatePages();
      });
      $scope.$watch('InspectDataService.limit', function(newVal, oldVal, scope) {
        if (newVal) {
          $scope.limit = newVal;
        }
        return _this.recalculatePages();
      });
      return $scope.$watch('InspectDataService.status', function(newVal, oldVal, scope) {
        return _this.recalculatePages();
      });
    };
    this.offsetFromPage = function(page) {
      var _ref;
      return page * ((_ref = $scope.limit) != null ? _ref : InspectDataService.defaultLimit);
    };
    /**
     * Recalculates all the vital variables.
     * Is called internally after all external changes to sensitive variables.
     *
     * @function recalculatePages
     * @memberOf InspectPaginationController
     *
    */

    this.recalculatePages = function() {
      var newLastPage, newPageOffset;
      if (!$scope.limit) {
        newPageOffset = 0;
        newLastPage = 0;
      } else {
        newPageOffset = Math.ceil($scope.offset / $scope.limit);
        newLastPage = Math.floor($scope.lastRow / $scope.limit);
      }
      $scope.pageOffset = newPageOffset;
      $scope.lastPage = newLastPage;
      $scope.isLoading = $scope.InspectDataService.isLoading;
      $scope.canGoToNextPage = !$scope.isLoading && $scope.pageOffset < $scope.lastPage;
      return $scope.canGoToPrevPage = !$scope.isLoading && $scope.pageOffset > 0;
    };
    /**
     * Refetches data according to set params.
     *
     * @function $scope.fetch
     * @memberOf InspectPaginationController
     *
    */

    $scope.fetch = function() {
      InspectDataService.offset = _this.offsetFromPage($scope.pageOffset);
      InspectDataService.limit = $scope.limit;
      _this.recalculatePages();
      return InspectDataService.fetch();
    };
    /**
     * Fetches next page, if available.
     *
     * @function $scope.nextPage
     * @memberOf InspectPaginationController
     *
    */

    $scope.nextPage = function() {
      if (!$scope.canGoToNextPage) {
        return;
      }
      $scope.pageOffset += 1;
      return $scope.fetch();
    };
    /**
     * Fetches previous page, if available.
     *
     * @function $scope.prevPage
     * @memberOf InspectPaginationController
     *
    */

    $scope.prevPage = function() {
      if (!$scope.canGoToPrevPage) {
        return;
      }
      $scope.pageOffset -= 1;
      return $scope.fetch();
    };
    /**
     * Returns tooltip hint for the particular page number.
     *
     * @function $scope.pageSliderTooltipValue
     * @memberOf InspectPaginationController
     *
    */

    $scope.pageSliderTooltipValue = function(pageOffset) {
      return "row " + (_this.offsetFromPage(pageOffset));
    };
    return this.init();
  });

}).call(this);

/*** @module h2o.controllers.parse **/


(function() {
  var module;

  module = angular.module('h2o.controllers.parse');

  module.controller('ParseAppController', function($state, $scope, $rootScope, $log, ParsePreviewService, ParseSettingsService, ParseJobStatusDataService, StatisticsPollerDataService, MenuService) {
    $scope.MenuService = MenuService;
    $scope.ParseSettingsService = ParseSettingsService;
    this.init = function() {
      this.setupLocalStorageStateSaving();
      this.setupPrerequisitesCheck();
      return this.setupServicesStopAndGo();
    };
    this.setupPrerequisitesCheck = function() {
      var _this = this;
      return $rootScope.$on('$stateChangeStart', function(event, toState, toParams, fromState, fromParams) {
        var breakAndTransitionToState;
        if (event.defaultPrevented) {
          return;
        }
        breakAndTransitionToState = function(newState) {
          event.preventDefault();
          return $state.go(newState, toParams);
        };
        if (toState.name === "choose_data_source") {

        } else if (toState.name === "filter_files") {
          if (!ParseSettingsService.hasFiles()) {
            $log.debug("[setupPrerequisitesCheck:" + toState.name + "] No files found in ParseSettingsService, returning back.");
            return breakAndTransitionToState('choose_data_source');
          }
        } else if (toState.name === "preview") {
          if (!ParseSettingsService.hasFiles()) {
            $log.debug("[setupPrerequisitesCheck:" + toState.name + "] No files found in ParseSettingsService, returning back.");
            return breakAndTransitionToState('choose_data_source');
          }
          if (!ParseSettingsService.hasFilesIncluded()) {
            $log.debug("[setupPrerequisitesCheck:" + toState.name + "] No included files found in ParseSettingsService, returning back.");
            return breakAndTransitionToState('filter_files');
          }
        } else if (toState.name === "progress") {
          if (!ParseSettingsService.hasFiles()) {
            $log.debug("[setupPrerequisitesCheck:" + toState.name + "] No files found in ParseSettingsService, returning back.");
            return breakAndTransitionToState('choose_data_source');
          }
          if (!ParseSettingsService.hasFilesIncluded()) {
            $log.debug("[setupPrerequisitesCheck:" + toState.name + "] No included files found in ParseSettingsService, returning back.");
            return breakAndTransitionToState('filter_files');
          }
          if (!ParseSettingsService.job) {
            $log.debug("[setupPrerequisitesCheck:" + toState.name + "] No job found in ParseSettingsService, returning back.");
            return breakAndTransitionToState('preview');
          }
        } else {

        }
      });
    };
    this.setupServicesStopAndGo = function() {
      var _this = this;
      return $rootScope.$on('$stateChangeSuccess', function(event, toState, toParams, fromState, fromParams) {
        if (event.defaultPrevented) {
          return;
        }
        if (toState.name === "progress") {
          $log.debug("Entered the progress page, starting polling.");
          ParseJobStatusDataService.startPolling();
          StatisticsPollerDataService.startPolling();
        } else {
          $log.debug("Left the progress page, stopping polling.");
          ParseJobStatusDataService.stopPolling();
          StatisticsPollerDataService.stopPolling();
        }
        if (toState.name === "preview") {
          $log.debug("Entered the preview page, starting preview refresh.");
          return ParsePreviewService.startRefreshingPreview();
        } else {
          $log.debug("Left the preview page, stopping preview refresh.");
          return ParsePreviewService.stopRefreshingPreview();
        }
      });
    };
    this.setupLocalStorageStateSaving = function() {
      var _this = this;
      return $rootScope.$on('$stateChangeStart', function(event, toState, toParams, fromState, fromParams) {
        var idInParams, idInState, needToLoadState, needToResetID, needToUpdateURI, savedStateExists;
        if (event.defaultPrevented) {
          return;
        }
        idInParams = toParams.wizardID;
        idInState = ParseSettingsService.wizardID;
        needToLoadState = (idInParams != null) && idInParams !== idInState;
        needToResetID = (idInParams == null) && (idInState == null);
        needToUpdateURI = (idInState != null) && (idInParams == null);
        if (needToLoadState) {
          $log.debug("Loading state from " + idInParams);
          savedStateExists = ParseSettingsService.loadFromLocalStorage(idInParams);
          if (savedStateExists) {
            ParseSettingsService.wizardID = idInParams;
            ParsePreviewService.refreshPreview();
          } else {
            needToResetID = true;
          }
        }
        if (needToResetID) {
          $log.debug("Setting up new wizard ID");
          ParseSettingsService.wizardID = uuid();
          needToUpdateURI = true;
        }
        if (needToUpdateURI) {
          $log.debug("Updating URI to match " + idInState);
          toParams.wizardID = ParseSettingsService.wizardID;
          event.preventDefault();
          return $state.go(toState, toParams);
        }
      });
    };
    this.init();
    return this;
  });

}).call(this);

/*** @module h2o.controllers.parse **/


(function() {
  var module;

  module = angular.module('h2o.controllers.parse');

  module.controller('ChooseDataSourceController', function($scope, $log, $state, ParseSettingsService, TypeaheadService, ParseFileUploaderService, ErrorAndStatusService) {
    $scope.ErrorAndStatusService = ErrorAndStatusService;
    $scope.ParseSettingsService = ParseSettingsService;
    $scope.ParseFileUploaderService = ParseFileUploaderService;
    $scope.isReadyToUpload = false;
    $scope.chooseDataSourceFormStartUploadMethod = function() {};
    return $scope.chooseDataSourceFormDidFinish = function(_arg) {
      var dst, files;
      files = _arg.files, dst = _arg.dst;
      ParseSettingsService.receivedInitialFilesAndDst(files, dst);
      if (files.length === 1) {
        return $state.go('preview');
      } else if (files.length > 1) {
        return $state.go('filter_files');
      }
    };
  });

  return;

}).call(this);

/*** @module h2o.controllers.parse **/


(function() {
  var module,
    __indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

  module = angular.module('h2o.controllers.parse');

  module.controller('FilterFilesController', function($scope, $log, $state, ParseSettingsService, ErrorAndStatusService) {
    var _this = this;
    $scope.ParseSettingsService = ParseSettingsService;
    $scope.filesCount = 0;
    $scope.includedFilesCount = 0;
    $scope.excludedFilesCount = 0;
    this.updateFileCounters = function() {
      var file;
      $scope.filesCount = ParseSettingsService.files.length;
      $scope.includedFilesCount = ((function() {
        var _i, _len, _ref, _results;
        _ref = ParseSettingsService.files;
        _results = [];
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          file = _ref[_i];
          if (!file.excluded) {
            _results.push(file);
          }
        }
        return _results;
      })()).length;
      return $scope.excludedFilesCount = ((function() {
        var _i, _len, _ref, _results;
        _ref = ParseSettingsService.files;
        _results = [];
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          file = _ref[_i];
          if (file.excluded) {
            _results.push(file);
          }
        }
        return _results;
      })()).length;
    };
    $scope.regexpExclusionMode = "exclude";
    $scope.regexpString = "";
    $scope.regexpStringIsValid = true;
    $scope.regexpSelectedIndices = [];
    $scope.regexpNotSelectedIndices = [];
    $scope.isShownInTheIncludeList = function(index) {
      return !ParseSettingsService.files[index].excluded;
    };
    $scope.isShownInTheExcludeList = function(index) {
      return ParseSettingsService.files[index].excluded;
    };
    $scope.isSelectedByRegexp = function(index) {
      return __indexOf.call($scope.regexpSelectedIndices, index) >= 0;
    };
    $scope.isNotSelectedByRegexp = function(index) {
      return __indexOf.call($scope.regexpNotSelectedIndices, index) >= 0;
    };
    $scope.regexpSubmit = function() {
      if ($scope.regexpExclusionMode === "exclude") {
        return $scope.excludeIndices($scope.regexpSelectedIndices);
      } else {
        return $scope.excludeIndices($scope.regexpNotSelectedIndices);
      }
    };
    $scope.includeIndices = function(indices) {
      var file, i, _i, _len, _results;
      _results = [];
      for (_i = 0, _len = indices.length; _i < _len; _i++) {
        i = indices[_i];
        file = ParseSettingsService.files[i];
        _results.push(file.excluded = false);
      }
      return _results;
    };
    $scope.excludeIndices = function(indices) {
      var file, i, _i, _len, _results;
      _results = [];
      for (_i = 0, _len = indices.length; _i < _len; _i++) {
        i = indices[_i];
        file = ParseSettingsService.files[i];
        _results.push(file.excluded = true);
      }
      return _results;
    };
    $scope.resetChanges = function() {
      return ParseSettingsService.resetToInitialFilesState();
    };
    $scope.goBack = function() {
      return $state.go('choose_data_source');
    };
    $scope.readyToGoNext = function() {
      if ($scope.includedFilesCount > 0) {
        return true;
      } else {
        return false;
      }
    };
    $scope.goNext = function() {
      if ($scope.readyToGoNext()) {
        return $state.go('preview');
      } else {
        ErrorAndStatusService.addError('chooseDataSourceForm', {
          message: 'No files included.'
        });
        return $log.debug("no files selected");
      }
    };
    this.updateRegexpSelectionList = function() {
      var anyMatch, e, file, i, matches, regexpMatcher, regexpString, _i, _len, _ref, _results;
      regexpString = $scope.regexpString;
      $scope.regexpSelectedIndices = [];
      $scope.regexpNotSelectedIndices = [];
      $scope.regexpStringIsValid = true;
      if (!regexpString) {
        return;
      }
      _ref = ParseSettingsService.files;
      _results = [];
      for (i = _i = 0, _len = _ref.length; _i < _len; i = ++_i) {
        file = _ref[i];
        try {
          regexpMatcher = new RegExp(regexpString, "gi");
        } catch (_error) {
          e = _error;
          $scope.regexpStringIsValid = false;
        }
        if (regexpMatcher == null) {
          continue;
        }
        matches = regexpMatcher.exec(file.lastPathComponent());
        anyMatch = matches != null;
        if (anyMatch) {
          $scope.regexpSelectedIndices.push(i);
          _results.push($log.debug("" + i + " -> " + matches + " in " + file.key));
        } else {
          _results.push($scope.regexpNotSelectedIndices.push(i));
        }
      }
      return _results;
    };
    this.init = function() {
      return this.setupRegexpFieldWatches();
    };
    this.setupRegexpFieldWatches = function() {
      var _this = this;
      $scope.$watch('regexpString', this.updateRegexpSelectionList);
      $scope.$watch('regexpExclusionMode', this.updateRegexpSelectionList);
      return $scope.$watch(function() {
        return ParseSettingsService.files;
      }, function() {
        _this.updateFileCounters();
        return _this.updateRegexpSelectionList();
      }, true);
    };
    this.init();
    return this;
  });

}).call(this);

/*** @module h2o.controllers.parse **/


(function() {
  var module;

  module = angular.module('h2o.controllers.parse');

  module.controller('PreviewController', function($scope, $state, $log, $timeout, $rootScope, ParsePreviewService, ParseGetHeaderService, ParseService, ParseSettingsService, ErrorAndStatusService, DialogService) {
    var _this = this;
    $scope.DialogService = DialogService;
    $scope.ErrorAndStatusService = ErrorAndStatusService;
    $scope.ParsePreviewService = ParsePreviewService;
    $scope.ParseSettingsService = ParseSettingsService;
    $scope.headerOptionsOpened = false;
    $scope.dataOptionsOpened = false;
    $scope.tableDataProvider = {
      isDataReady: function() {
        return (ParsePreviewService.data != null) && (ParseSettingsService.parserConfigData != null) && (ParseSettingsService.parserConfigData.columns != null);
      },
      getNumberOfRows: function() {
        if (this.isDataReady()) {
          return ParsePreviewService.data.length + 2;
        }
        return 0;
      },
      getNumberOfColumns: function() {
        if (this.isDataReady()) {
          return ParseSettingsService.parserConfigData.columns.length;
        }
        return 0;
      },
      getRow: function(rowIndex) {
        if (rowIndex <= 1) {
          return ParseSettingsService.parserConfigData.columns;
        } else {
          return ParsePreviewService.data[rowIndex - 2];
        }
      },
      getValue: function(rowIndex, colIndex) {
        var row;
        row = this.getRow(rowIndex);
        if (row == null) {
          return null;
        }
        return row[colIndex];
      },
      getValueForSizing: function(rowIndex, colIndex) {
        if (rowIndex <= 1) {
          return "XXXXXXXXXXXXX";
        }
        return ParsePreviewService.data[rowIndex - 2][colIndex];
      }
    };
    $scope.markColumnsAsDirty = function() {
      ParseSettingsService.userSetColumnNames = true;
      return ParseSettingsService.didChangeColumnSettings();
    };
    $scope.markColumnsToBeOverwritten = function() {
      return ParseSettingsService.userSetColumnNames = false;
    };
    $scope.didChangeHeaderFile = function() {
      if (ParseSettingsService.headerFile != null) {
        ParseSettingsService.skipHeader = false;
      }
      return $scope.markColumnsToBeOverwritten();
    };
    $scope.didChangeSkipHeader = function() {
      if (ParseSettingsService.skipHeader != null) {
        ParseSettingsService.headerFile = null;
      }
      return $scope.markColumnsToBeOverwritten();
    };
    $scope.isReadyToUpload = false;
    $scope.chooseDataSourceFormStartUploadMethod = function() {};
    $scope.chooseDataSourceDialogID = "chooseDataSourceDialogID";
    $scope.chooseDataSourceFormDidFinish = function(_arg) {
      var dst, files, r;
      files = _arg.files, dst = _arg.dst;
      r = ParseGetHeaderService.changeHeaderTo(files[0]);
      return r.then(function() {
        return DialogService.close($scope.chooseDataSourceDialogID);
      }, function() {
        return $log.debug("[ParseGetHeaderService.changeHeaderTo] Failed.");
      });
    };
    $scope.openHeaderOptions = function() {
      $scope.headerOptionsOpened = true;
      return ParseSettingsService.saveContext();
    };
    $scope.openDataOptions = function() {
      $scope.dataOptionsOpened = true;
      $rootScope.$broadcast('ParsePreviewDataChanged');
      return ParseSettingsService.saveContext();
    };
    $scope.okOptions = function() {
      $scope.dataOptionsOpened = false;
      $scope.headerOptionsOpened = false;
      return $rootScope.$broadcast('ParsePreviewDataChanged');
    };
    $scope.cancelOptions = function() {
      $scope.dataOptionsOpened = false;
      $scope.headerOptionsOpened = false;
      $rootScope.$broadcast('ParsePreviewDataChanged');
      return $timeout((function() {
        return ParseSettingsService.undoToPreviouslySavedContext();
      }), 1550);
    };
    $scope.startParse = function() {
      var r;
      r = ParseService.startParse();
      return r.then(function() {
        $log.debug("[PreviewController.startParse] Started! Received job id: " + ParseSettingsService.job + ".");
        return $state.go('progress');
      }, function() {
        return $log.error("[PreviewController.startParse] Failed.");
      });
    };
    return this;
  });

}).call(this);

/*** @module h2o.controllers.parse **/


(function() {
  var module;

  module = angular.module('h2o.controllers.parse');

  module.controller('ProgressController', function($log, $scope, $rootScope, $state, $stateParams, ParseJobCancelService, ParseService, ParseSettingsService, ParseJobStatusDataService, StatisticsPollerDataService, ErrorAndStatusService) {
    var _this = this;
    $scope.ErrorAndStatusService = ErrorAndStatusService;
    $scope.ParseSettingsService = ParseSettingsService;
    $scope.ParseJobStatusDataService = ParseJobStatusDataService;
    $scope.StatisticsPollerDataService = StatisticsPollerDataService;
    this.init = function() {
      return $rootScope.$watch((function() {
        return ParseJobStatusDataService.isComplete();
      }), this.parseDidCompleteSuccessfully);
    };
    this.parseDidCompleteSuccessfully = function() {
      if (ParseJobStatusDataService.isComplete() && !ParseJobStatusDataService.hasErrors()) {
        $log.info("[ProgressController parseDidCompleteSuccessfully]");
        $scope.goToInspect();
      }
    };
    $scope.goToInspect = function() {
      var dst;
      dst = ParseSettingsService.dst;
      return window.location.href = "Inspect.html?key=" + dst;
    };
    $scope.cancelParse = function() {
      var r;
      r = ParseJobCancelService.cancel();
      return r.then(function() {
        $log.debug("[ProgressController.cancelParse] Job with id " + ParseSettingsService.job + " has been cancelled.");
        return $state.go('preview');
      }, function() {
        return $log.error("[ProgressController.cancelParse] Failed.");
      });
    };
    $scope.retryWithoutErrorneousFiles = function() {
      var r;
      ParseJobStatusDataService.excludeErrorneousFiles();
      r = ParseService.startParse();
      return r.then(function() {
        $log.debug("[ProgressController.retryWithoutErrorneousFiles] Started! Received job id: " + ParseSettingsService.job + ".");
        return $state.go('progress', $stateParams, {
          reload: true
        });
      }, function() {
        $log.error("[ProgressController.retryWithoutErrorneousFiles] Failed.");
        return $state.go('progress', $stateParams, {
          reload: true
        });
      });
    };
    this.init();
    return this;
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.directives.inspect');

  module.directive("dataservicespinner", function(InspectDataService) {
    return {
      restrict: "E",
      template: '<div class="spinner-wrapper"><div class="spinner"><i class="icon-spinner icon-spin icon-large"></i><button class="retry-button">Retry <i class="icon-repeat"></i></button></div></div>',
      replace: true,
      link: function(scope, element, attrs) {
        var jqElement, jqRetryButton, jqSpinner, updateElement;
        jqElement = $(element);
        jqSpinner = jqElement.find("i.icon-spinner");
        jqRetryButton = jqElement.find("button");
        jqRetryButton.click(function() {
          return InspectDataService.fetch();
        });
        updateElement = function() {
          if (InspectDataService.status === HTTP_STATUS.loading || InspectDataService.status === HTTP_STATUS.error) {
            jqElement.stop().fadeIn();
          } else {
            jqElement.stop().fadeOut();
          }
          if (InspectDataService.status === HTTP_STATUS.error) {
            jqSpinner.hide();
            return jqRetryButton.show();
          } else {
            jqSpinner.show();
            return jqRetryButton.hide();
          }
        };
        return scope.$watch('InspectDataService.status', updateElement);
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.directives.inspect');

  module.directive("dataservicestatus", function(InspectDataService) {
    return {
      restrict: "A",
      link: function(scope, element, attrs) {
        var elementClass, elementText, jqElement, updateElement;
        jqElement = $(element);
        elementClass = function() {
          if (InspectDataService.status === HTTP_STATUS.ok) {
            return "ok";
          }
          if (InspectDataService.status === HTTP_STATUS.loading) {
            return "loading";
          }
          if (InspectDataService.status === HTTP_STATUS.error) {
            return "error";
          }
          return "";
        };
        elementText = function() {
          var miliseconds, seconds;
          if (InspectDataService.status === HTTP_STATUS.ok) {
            miliseconds = InspectDataService.meta.processingTime;
            seconds = miliseconds / 1000.0;
            if (miliseconds < 5000) {
              return "Produced in " + (seconds.toFixed(3)) + " sec.";
            } else {
              return "Produced in " + (moment.duration(miliseconds).humanize()) + ".";
            }
          }
          if (InspectDataService.status === HTTP_STATUS.loading) {
            return "Loading...";
          }
          if (InspectDataService.status === HTTP_STATUS.error) {
            return "" + InspectDataService.error;
          }
          return "";
        };
        updateElement = function() {
          jqElement.removeClass('ok').removeClass('loading').removeClass('error');
          jqElement.addClass(elementClass());
          return jqElement.html("<div class=\"message\">" + (elementText()));
        };
        scope.$watch('InspectDataService.meta', updateElement);
        scope.$watch('InspectDataService.error', updateElement);
        return scope.$watch('InspectDataService.status', updateElement);
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.directives.inspect');

  module.directive("inspectTable", function($parse) {
    return {
      restrict: 'A',
      link: function(scope, element, attrs) {
        var datasetModel, headerClassmapModel, headersModel, reload, summariesModel, tbody, thead, thead_tr, vis;
        datasetModel = $parse(attrs.dataset);
        headersModel = $parse(attrs.headers);
        summariesModel = $parse(attrs.summaries);
        headerClassmapModel = $parse(attrs.headerClassmap);
        vis = d3.select(element[0]);
        thead = vis.append("thead");
        tbody = vis.append("tbody");
        thead_tr = thead.append("tr");
        reload = function() {
          var dataCells, dataCellsFuncs, dataRows, dataRowsFuncs, prepareRowFunction, prepareSummaryFunction, summaryCells, summaryCellsFuncs, summaryRows, summaryRowsFuncs, thFuncs, update,
            _this = this;
          if ((headersModel(scope) == null) || (datasetModel(scope) == null)) {
            return;
          }
          prepareSummaryFunction = function(row) {
            return function(column) {
              var klass, value;
              if (column.name === "row") {
                value = row[1];
              } else {
                value = column[row[0]];
              }
              if (!column.shown) {
                klass = "hidden";
              } else {
                if (value !== "0" && value !== 0 && row[1] === 'N/A' && value !== "N/A") {
                  klass = "na";
                }
              }
              return {
                klass: klass,
                value: value
              };
            };
          };
          prepareRowFunction = function(row) {
            return function(column) {
              var klass, value;
              value = row[column.name];
              klass = "";
              if (!column.shown) {
                klass = "hidden";
              } else {
                if (value === "NA") {
                  klass = "na";
                }
              }
              return {
                value: value,
                klass: klass
              };
            };
          };
          update = function(items, funcDict) {
            var added, removed, updated;
            added = items.enter();
            removed = items.exit();
            updated = items;
            funcDict.update(items);
            funcDict.update(funcDict.style(funcDict.add(added)));
            return funcDict.remove(removed);
          };
          thFuncs = {
            add: function(item) {
              return item.append("th");
            },
            remove: function(item) {
              return item.remove();
            },
            update: function(item) {
              return item.html(function(c) {
                return "<i></i>" + c.visual;
              }).attr("data-header", function(c) {
                return c.name;
              }).attr("class", function(c) {
                var _ref;
                return (_ref = headerClassmapModel(scope)[c.name]) != null ? _ref : "";
              }).style("display", function(c) {
                var _ref;
                if ((_ref = c.shown) != null ? _ref : true) {
                  return "table-cell";
                } else {
                  return "none";
                }
              });
            },
            style: function(item) {
              return item;
            }
          };
          summaryRowsFuncs = {
            add: function(item) {
              return item.append("tr");
            },
            remove: function(item) {
              return item.remove();
            },
            update: function(item) {
              return item;
            },
            style: function(item) {
              return item.attr("data-type", "summary").attr('class', function(c) {
                return "summary " + c[0];
              });
            }
          };
          dataRowsFuncs = {
            add: function(item) {
              return item.append("tr");
            },
            remove: function(item) {
              return item.remove();
            },
            update: function(item) {
              return item;
            },
            style: function(item) {
              return item.attr("data-type", "data").attr('class', function(c) {
                return "row" + c.row;
              });
            }
          };
          summaryCellsFuncs = {
            add: function(item) {
              return item.append("td");
            },
            remove: function(item) {
              return item.remove();
            },
            update: function(item) {
              return item.attr('class', function(item) {
                return item.klass;
              }).html(function(item) {
                return item.value;
              });
            },
            style: function(item) {
              return item;
            }
          };
          dataCellsFuncs = {
            add: function(item) {
              return item.append("td");
            },
            remove: function(item) {
              return item.remove();
            },
            update: function(item) {
              return item.attr('class', function(item) {
                return item.klass;
              }).html(function(item) {
                return item.value;
              });
            },
            style: function(item) {
              return item;
            }
          };
          update(thead_tr.selectAll("th").data(headersModel(scope)), thFuncs);
          summaryRows = tbody.selectAll("tr[data-type=\"summary\"]").data(summariesModel(scope));
          dataRows = tbody.selectAll("tr[data-type=\"data\"]").data(datasetModel(scope));
          update(summaryRows, summaryRowsFuncs);
          update(dataRows, dataRowsFuncs);
          summaryCells = summaryRows.selectAll("td").data(function(row) {
            return headersModel(scope).map(prepareSummaryFunction(row));
          });
          dataCells = dataRows.selectAll("td").data(function(row) {
            return headersModel(scope).map(prepareRowFunction(row));
          });
          update(summaryCells, summaryCellsFuncs);
          update(dataCells, dataCellsFuncs);
          return element.trigger("didReload");
        };
        return scope.$watch(headersModel, function(newVal, oldVal) {
          if (!newVal) {
            return;
          }
          return reload();
        });
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('swing.directives.parse');

  module.directive("chooseDataSourceForm", function($log, $rootScope, $upload, TypeaheadService, ParseFileUploaderService, ErrorAndStatusService) {
    return {
      restrict: "E",
      templateUrl: "templates/chooseDataSourceFormDirective.html",
      scope: {
        onFailureCallback: '=',
        onSuccessCallback: '=',
        isReadyModel: '=',
        saveProceedMethodIn: '=',
        multiple: '='
      },
      controller: function($scope, $element) {
        $scope.TypeaheadService = TypeaheadService;
        $scope.uris = {
          http: "",
          hdfs: "",
          s3: "",
          cluster: ""
        };
        return $scope.currentTab = 'http';
      },
      link: function($scope, element, $attrs) {
        var getCurrentURI, getUploadedFilesCount, isFileUpload, isURIUpload, uploadFilesFromLocal, uploadFilesFromURI,
          _this = this;
        $scope.files = [];
        $scope.currentUpload = null;
        $scope.updateFiles = function($files) {
          if ($scope.currentUpload != null) {
            $scope.currentUpload.abort();
          }
          $scope.files = [];
          return $scope.files = $files;
        };
        getUploadedFilesCount = function() {
          if ($scope.files != null) {
            return $scope.files.length;
          } else {
            return 0;
          }
        };
        getCurrentURI = function() {
          var uri;
          uri = $scope.uris[$scope.currentTab];
          if (uri.value != null) {
            return uri.value;
          } else {
            return uri;
          }
        };
        isFileUpload = function() {
          return $scope.currentTab === 'fileUpload';
        };
        isURIUpload = function() {
          return !isFileUpload() && (getCurrentURI() != null);
        };
        $scope.setCurrentTab = function(tabName) {
          return $scope.currentTab = tabName;
        };
        $scope.saveProceedMethodIn = function() {
          if (!$scope.isReadyModel) {
            ErrorAndStatusService.addError('chooseDataSourceForm', {
              message: 'Please enter file(s) URI'
            });
            return;
          }
          if (isFileUpload()) {
            return uploadFilesFromLocal();
          } else if (isURIUpload()) {
            return uploadFilesFromURI();
          }
        };
        $scope.updateIsReady = function() {
          if (isURIUpload() && getCurrentURI().length > 0) {
            return $scope.isReadyModel = true;
          } else if (isFileUpload() && getUploadedFilesCount() > 0) {
            return $scope.isReadyModel = true;
          } else {
            return $scope.isReadyModel = false;
          }
        };
        $scope.$watch((function() {
          return $scope.files;
        }), $scope.updateIsReady, true);
        $scope.$watch((function() {
          return $scope.uris;
        }), $scope.updateIsReady, true);
        $scope.$watch((function() {
          return $scope.currentTab;
        }), $scope.updateIsReady);
        uploadFilesFromURI = function() {
          _this.waitForFilesPromise = ParseFileUploaderService.loadFilesFromURI(getCurrentURI(), $scope.currentTab);
          return _this.waitForFilesPromise.then(function(_arg) {
            var dst, files;
            files = _arg.files, dst = _arg.dst;
            $log.debug('SUCCESS SERVICE PROMISE');
            if ($scope.onSuccessCallback != null) {
              return $scope.onSuccessCallback({
                files: files,
                dst: dst
              });
            }
          }, function() {
            $log.debug('ERROR SERVICE PROMISE');
            if ($scope.onFailureCallback != null) {
              return $scope.onFailureCallback();
            }
          });
        };
        return uploadFilesFromLocal = function() {
          var file, _i, _len, _ref, _results;
          if ($scope.currentUpload != null) {
            $scope.currentUpload.abort();
          }
          _this.waitForFilesPromise = ParseFileUploaderService.waitForFiles(getUploadedFilesCount());
          _this.waitForFilesPromise.then(function(_arg) {
            var dst, files;
            files = _arg.files, dst = _arg.dst;
            $log.debug('SUCCESS SERVICE PROMISE');
            if ($scope.onSuccessCallback != null) {
              return $scope.onSuccessCallback({
                files: files,
                dst: dst
              });
            }
          }, function() {
            $log.debug('ERROR SERVICE PROMISE');
            if ($scope.onFailureCallback != null) {
              return $scope.onFailureCallback();
            }
          });
          _ref = $scope.files;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            file = _ref[_i];
            $log.debug("url: " + ParseFileUploaderService.postFileEndpoint + "?filename=" + encodeURIComponent(file.name));
            _results.push($scope.currentUpload = $upload.upload({
              url: ParseFileUploaderService.postFileEndpoint + "?filename=" + encodeURIComponent(file.name),
              file: file,
              fileFormDataName: 'file'
            }).success(function(data, status, headers, config) {
              var isError;
              ParseFileUploaderService.fileUploadGotResponse(file.name, data, isError = false);
              return $log.debug(data);
            }).error(function(data, status, headers, config) {
              var isError;
              ParseFileUploaderService.fileUploadGotResponse(file.name, data, isError = true);
              return $log.error(data);
            }));
          }
          return _results;
        };
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.directives');

  module.directive("virtualtable", function($parse, $rootScope) {
    return {
      restrict: 'E',
      scope: true,
      compile: function(tElement, tAttrs) {
        var $container, $containerScrollMaker, $window, $wrapper;
        $wrapper = $('<div class="virtualtable wrapper"></div>');
        $container = $('<div class="virtualtable container"></div>');
        $containerScrollMaker = $("<div>&nbsp;</div>");
        $window = $('<div class="virtualtable window"></div>');
        $window.append(tElement.children());
        $container.append($window);
        $container.append($containerScrollMaker);
        $wrapper.append($container);
        tElement.append($wrapper);
        return this.link;
      },
      link: function(scope, element, attrs) {
        var $container, $element, $window, $wrapper, cacheKey, colOriginX, columnWidths, dataProviderFn, getWindow, isDirty, leftOffsetPx, letterWidthPx, minColumnWidthPx, options, preloadMarginPx, prevColRange, prevRowRange, prevScrollMaxX, prevScrollMaxY, prevWindowSize, recalculateWidths, refreshEvent, reload, resolveDataProvider, rowHeightPx, rowOriginY, scrollFactorXFn, scrollFactorYFn, scrollMaxX, scrollMaxY, sizeProvider, topOffsetPx, updateView, _ref, _ref1, _ref2, _ref3, _ref4, _ref5, _ref6;
        $element = $(element[0]);
        $wrapper = $($element.find(".wrapper"));
        $container = $($element.find(".container"));
        $window = $($element.find(".window"));
        scope.rows = [];
        dataProviderFn = $parse(attrs.provider);
        refreshEvent = attrs.refreshEvent;
        options = (_ref = scope.$eval(attrs.options)) != null ? _ref : {};
        letterWidthPx = (_ref1 = options.letterWidthPx) != null ? _ref1 : 8.0;
        rowHeightPx = (_ref2 = options.rowHeightPx) != null ? _ref2 : 100.0;
        preloadMarginPx = (_ref3 = options.preloadMarginPx) != null ? _ref3 : 0.0;
        minColumnWidthPx = (_ref4 = options.minColumnWidthPx) != null ? _ref4 : 0.0;
        leftOffsetPx = (_ref5 = options.leftOffsetPx) != null ? _ref5 : 0.0;
        topOffsetPx = (_ref6 = options.topOffsetPx) != null ? _ref6 : 0.0;
        resolveDataProvider = function() {
          return dataProviderFn(scope);
        };
        columnWidths = {};
        colOriginX = {};
        rowOriginY = {};
        scrollMaxX = 0;
        scrollMaxY = 0;
        scrollFactorXFn = function() {
          return 1;
        };
        scrollFactorYFn = function() {
          return 1;
        };
        prevScrollMaxX = 0;
        prevScrollMaxY = 0;
        sizeProvider = {
          getWidth: function(colIndex) {
            return columnWidths[colIndex];
          },
          getHeight: function(rowIndex) {
            return rowHeightPx;
          },
          getOriginX: function(colIndex) {
            return colOriginX[colIndex];
          },
          getOriginY: function(rowIndex) {
            return rowOriginY[rowIndex];
          },
          getSizeX: function(fromIndex, toIndex) {
            return this.getOriginX(toIndex) + this.getWidth(toIndex) - this.getOriginX(fromIndex);
          },
          getSizeY: function(fromIndex, toIndex) {
            return this.getOriginY(toIndex) + this.getHeight(toIndex) - this.getOriginY(fromIndex);
          }
        };
        recalculateWidths = function() {
          var colCount, colIndex, colValue, dataProvider, maxAllowedScrollValue, originX, originY, originalScrollMaxX, originalScrollMaxY, rowCount, rowIndex, thisValueWidth, _i, _j, _k, _l, _m;
          dataProvider = resolveDataProvider();
          rowCount = dataProvider.getNumberOfRows();
          colCount = dataProvider.getNumberOfColumns();
          columnWidths = {};
          for (colIndex = _i = 0; 0 <= colCount ? _i < colCount : _i > colCount; colIndex = 0 <= colCount ? ++_i : --_i) {
            columnWidths[colIndex] = minColumnWidthPx;
          }
          for (rowIndex = _j = 0; 0 <= rowCount ? _j < rowCount : _j > rowCount; rowIndex = 0 <= rowCount ? ++_j : --_j) {
            for (colIndex = _k = 0; 0 <= colCount ? _k < colCount : _k > colCount; colIndex = 0 <= colCount ? ++_k : --_k) {
              colValue = dataProvider.getValueForSizing(rowIndex, colIndex);
              thisValueWidth = (colValue.length + 5) * letterWidthPx;
              if (thisValueWidth > columnWidths[colIndex]) {
                columnWidths[colIndex] = thisValueWidth;
              }
            }
          }
          colOriginX = {};
          rowOriginY = {};
          maxAllowedScrollValue = 1000000;
          originY = topOffsetPx;
          for (rowIndex = _l = 0; 0 <= rowCount ? _l < rowCount : _l > rowCount; rowIndex = 0 <= rowCount ? ++_l : --_l) {
            rowOriginY[rowIndex] = originY;
            originY += sizeProvider.getHeight(rowIndex);
          }
          originX = leftOffsetPx;
          for (colIndex = _m = 0; 0 <= colCount ? _m < colCount : _m > colCount; colIndex = 0 <= colCount ? ++_m : --_m) {
            colOriginX[colIndex] = originX;
            originX += sizeProvider.getWidth(colIndex);
          }
          scrollMaxX = originX;
          scrollMaxY = originY;
          if (prevScrollMaxX !== scrollMaxX || prevScrollMaxY !== scrollMaxY) {
            scrollFactorXFn = function() {
              return 1;
            };
            if (scrollMaxX > maxAllowedScrollValue) {
              originalScrollMaxX = scrollMaxX;
              scrollFactorXFn = function() {
                return (originalScrollMaxX - $wrapper.width()) / (maxAllowedScrollValue - $wrapper.width());
              };
              scrollMaxX = maxAllowedScrollValue;
            }
            scrollFactorYFn = function() {
              return 1;
            };
            if (scrollMaxY > maxAllowedScrollValue) {
              originalScrollMaxY = scrollMaxY;
              scrollFactorYFn = function() {
                return (originalScrollMaxY - $wrapper.height()) / (maxAllowedScrollValue - $wrapper.height());
              };
              scrollMaxY = maxAllowedScrollValue;
            }
            $container.css({
              width: "" + scrollMaxX + "px",
              height: "" + scrollMaxY + "px"
            });
          }
          prevScrollMaxX = scrollMaxX;
          return prevScrollMaxY = scrollMaxY;
        };
        prevWindowSize = null;
        getWindow = function() {
          var bottomBoundary, colCount, colIndex, colRange, currentX, currentY, dataProvider, leftBoundary, leftmostItemLeft, rightBoundary, rowCount, rowIndex, rowRange, scrollLeft, scrollTop, topBoundary, topmostItemTop, transformedScrollLeft, transformedScrollTop, viewOrigin, windowOrigin, windowSize, _i, _j;
          dataProvider = resolveDataProvider();
          rowCount = dataProvider.getNumberOfRows();
          colCount = dataProvider.getNumberOfColumns();
          scrollLeft = $wrapper.scrollLeft();
          scrollTop = $wrapper.scrollTop();
          transformedScrollLeft = Math.floor(scrollLeft * scrollFactorXFn());
          transformedScrollTop = Math.floor(scrollTop * scrollFactorYFn());
          leftBoundary = transformedScrollLeft - preloadMarginPx;
          topBoundary = transformedScrollTop - preloadMarginPx;
          rightBoundary = transformedScrollLeft + $wrapper.width() + preloadMarginPx;
          bottomBoundary = transformedScrollTop + $wrapper.height() + preloadMarginPx;
          rowRange = [0, 0];
          colRange = [0, 0];
          currentY = 0;
          for (rowIndex = _i = 0; 0 <= rowCount ? _i < rowCount : _i > rowCount; rowIndex = 0 <= rowCount ? ++_i : --_i) {
            if (currentY < topBoundary) {
              rowRange[0] = rowIndex > 0 ? rowIndex - 1 : 0;
            }
            currentY += sizeProvider.getHeight(rowIndex);
            if (currentY < bottomBoundary) {
              rowRange[1] = rowIndex > (rowCount - 2) ? rowCount - 1 : rowIndex + 1;
            } else {
              break;
            }
          }
          currentX = 0;
          for (colIndex = _j = 0; 0 <= colCount ? _j < colCount : _j > colCount; colIndex = 0 <= colCount ? ++_j : --_j) {
            if (currentX < leftBoundary) {
              colRange[0] = colIndex > 0 ? colIndex - 1 : 0;
            }
            currentX += sizeProvider.getWidth(colIndex);
            if (currentX < rightBoundary) {
              colRange[1] = colIndex > (colCount - 2) ? colCount - 1 : colIndex + 1;
            } else {
              break;
            }
          }
          leftmostItemLeft = sizeProvider.getOriginX(colRange[0]) - leftOffsetPx;
          topmostItemTop = sizeProvider.getOriginY(rowRange[0]) - topOffsetPx;
          viewOrigin = [leftmostItemLeft - transformedScrollLeft + scrollLeft, topmostItemTop - transformedScrollTop + scrollTop];
          windowOrigin = [leftmostItemLeft, topmostItemTop];
          windowSize = [sizeProvider.getSizeX(colRange[0], colRange[1]) + leftOffsetPx, sizeProvider.getSizeY(rowRange[0], rowRange[1]) + topOffsetPx];
          if ((prevWindowSize == null) || isNaN(prevWindowSize[0]) || isNaN(prevWindowSize[1]) || windowSize[0] !== prevWindowSize[0] || windowSize[1] !== prevWindowSize[1]) {
            $window.css({
              width: "" + windowSize[0] + "px",
              height: "" + windowSize[1] + "px"
            });
            prevWindowSize = angular.copy(windowSize);
          }
          return {
            rowRange: rowRange,
            colRange: colRange,
            viewOrigin: viewOrigin,
            windowOrigin: windowOrigin
          };
        };
        isDirty = false;
        prevRowRange = null;
        prevColRange = null;
        cacheKey = 1;
        updateView = function() {
          var colRange, cssTransformValue, dataProvider, dataWindow, rowRange, updateScope, viewOrigin, windowOrigin;
          dataProvider = resolveDataProvider();
          dataWindow = getWindow();
          rowRange = dataWindow.rowRange;
          colRange = dataWindow.colRange;
          viewOrigin = dataWindow.viewOrigin;
          windowOrigin = dataWindow.windowOrigin;
          cssTransformValue = "translate(" + viewOrigin[0] + "px," + viewOrigin[1] + "px)";
          $window.css({
            "-webkit-transform": cssTransformValue,
            "-moz-transform": cssTransformValue,
            "-ms-transform": cssTransformValue,
            "-o-transform": cssTransformValue,
            "transform": cssTransformValue
          });
          updateScope = function() {
            var colIndex, row, rowIndex, _i, _j, _ref10, _ref7, _ref8, _ref9, _results;
            scope.rows = [];
            if (!dataProvider.getNumberOfColumns() || !dataProvider.getNumberOfRows()) {
              return;
            }
            _results = [];
            for (rowIndex = _i = _ref7 = rowRange[0], _ref8 = rowRange[1]; _ref7 <= _ref8 ? _i <= _ref8 : _i >= _ref8; rowIndex = _ref7 <= _ref8 ? ++_i : --_i) {
              row = {
                hash: "" + rowIndex + "_" + colRange[0] + "_" + colRange[1] + "_" + cacheKey,
                height: sizeProvider.getHeight(rowIndex),
                index: rowIndex,
                cells: [],
                offsetY: sizeProvider.getOriginY(rowIndex) - windowOrigin[1]
              };
              for (colIndex = _j = _ref9 = colRange[0], _ref10 = colRange[1]; _ref9 <= _ref10 ? _j <= _ref10 : _j >= _ref10; colIndex = _ref9 <= _ref10 ? ++_j : --_j) {
                row.cells.push({
                  hash: "" + rowIndex + "_" + colIndex + "_" + cacheKey,
                  offsetX: sizeProvider.getOriginX(colIndex) - windowOrigin[0],
                  width: sizeProvider.getWidth(colIndex),
                  index: colIndex,
                  value: dataProvider.getValue(rowIndex, colIndex)
                });
              }
              _results.push(scope.rows.push(row));
            }
            return _results;
          };
          if (isDirty) {
            cacheKey += 1;
          }
          if (isDirty || !angular.equals(prevRowRange, rowRange) || !angular.equals(prevColRange, colRange)) {
            if (scope.$$phase || scope.$root.$$phase) {
              updateScope();
            } else {
              scope.$apply(function() {
                return updateScope();
              });
            }
            isDirty = false;
            prevRowRange = angular.copy(rowRange);
            return prevColRange = angular.copy(colRange);
          }
        };
        reload = function() {
          isDirty = true;
          recalculateWidths();
          return updateView();
        };
        $wrapper.bind('scroll', updateView);
        $(window).bind('resize', updateView);
        $element.bind('AdjustHeightFired', updateView);
        scope.$on('$destroy', function(e) {
          $wrapper.unbind('scroll', updateView);
          $(window).unbind('resize', updateView);
          return $element.unbind('AdjustHeightFired', updateView);
        });
        $rootScope.$on(refreshEvent, function() {
          return reload();
        });
        return reload();
      }
    };
  });

}).call(this);

(function() {
  var module,
    __indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

  module = angular.module('h2o.directives');

  module.directive("autoradio", function($parse) {
    return {
      restrict: 'E',
      scope: {
        model: '=',
        checkedIfValue: '@',
        checkedIfValueNot: '@',
        checkedIfValueIn: '@',
        checkedIfValueNotIn: '@',
        onSelectionSetValue: '@'
      },
      replace: true,
      controller: function($scope) {
        var _this = this;
        $scope.isChecked = "no";
        $scope.evaluatedCheckedIfValue = $parse($scope.checkedIfValue);
        $scope.evaluatedCheckedIfValueNot = $parse($scope.checkedIfValueNot);
        $scope.evaluatedCheckedIfValueIn = $parse($scope.checkedIfValueIn);
        $scope.evaluatedCheckedIfValueNotIn = $parse($scope.checkedIfValueNotIn);
        $scope.evaluatedOnSelectionSetValue = $parse($scope.onSelectionSetValue);
        this.updateCheckedStatus = function() {
          var shouldBeOn, _ref, _ref1;
          shouldBeOn = false;
          if ($scope.evaluatedCheckedIfValue() != null) {
            shouldBeOn = $scope.model === $scope.evaluatedCheckedIfValue();
          } else if ($scope.evaluatedCheckedIfValueNot() != null) {
            shouldBeOn = $scope.model !== $scope.evaluatedCheckedIfValueNot();
          } else if ($scope.evaluatedCheckedIfValueIn() != null) {
            shouldBeOn = (_ref = $scope.model, __indexOf.call($scope.evaluatedCheckedIfValueIn(), _ref) >= 0);
          } else if ($scope.evaluatedCheckedIfValueNotIn() != null) {
            shouldBeOn = (_ref1 = $scope.model, __indexOf.call($scope.evaluatedCheckedIfValueNotIn(), _ref1) < 0);
          }
          return $scope.isChecked = shouldBeOn ? "yes" : "no";
        };
        $scope.$watch("model", this.updateCheckedStatus);
        return $scope.$watch("isChecked", function() {
          var isValueSpecified, shouldSetModelValue;
          shouldSetModelValue = $scope.isChecked === "yes";
          isValueSpecified = $scope.evaluatedOnSelectionSetValue() != null;
          if (shouldSetModelValue && isValueSpecified) {
            $scope.model = $scope.evaluatedOnSelectionSetValue();
          }
          return _this.updateCheckedStatus();
        });
      },
      template: '<input type="radio" ng-model="isChecked" value="yes">'
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.directives');

  module.directive("slider", function() {
    return {
      restrict: "E",
      template: "<div class=\"slider\"></div>",
      replace: true,
      link: function(scope, element, attrs) {
        var dragSliderArgs, hasTooltip, jqElement, jqHandleElement, slider, theTooltip, tooltipValue, updateSlider;
        tooltipValue = function(value) {
          return scope[attrs.tooltipValue](value);
        };
        jqElement = $(element);
        hasTooltip = attrs.tooltipValue != null;
        theTooltip = null;
        dragSliderArgs = {
          animate: true,
          range: 'min',
          min: 0,
          max: 0,
          value: 0,
          step: 1,
          change: function(event, ui) {
            var newOffset;
            newOffset = ui.value;
            if (newOffset === scope[attrs.offset]) {
              return;
            }
            return scope.$apply(function() {
              scope[attrs.offset] = newOffset;
              return scope[attrs.rsChange]();
            });
          }
        };
        if (hasTooltip) {
          $.extend(dragSliderArgs, {
            start: function(event, ui) {
              return jqHandleElement.tooltipster("show");
            },
            slide: function(event, ui) {
              jqHandleElement.tooltipster("update", tooltipValue(ui.value));
              return jqHandleElement.tooltipster("reposition");
            },
            stop: function(event, ui) {
              return jqHandleElement.tooltipster("hide");
            }
          });
        }
        slider = jqElement.dragslider(dragSliderArgs);
        jqHandleElement = $(jqElement.find("a"));
        if (hasTooltip) {
          jqHandleElement.tooltipster({
            delay: 0,
            speed: 100,
            updateAnimation: false,
            trigger: "custom",
            content: tooltipValue(0)
          });
        }
        updateSlider = function() {
          return slider.dragslider({
            min: scope[attrs.from],
            max: scope[attrs.to],
            value: scope[attrs.offset],
            disabled: scope[attrs.disabled]
          });
        };
        scope.$watch(attrs.from, updateSlider);
        scope.$watch(attrs.to, updateSlider);
        scope.$watch(attrs.offset, updateSlider);
        return scope.$watch(attrs.disabled, updateSlider);
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.directives');

  module.directive("dragHelperClass", function() {
    return {
      restrict: 'A',
      priority: 2,
      link: function(scope, element, attrs) {
        var $element;
        $element = $(element);
        return $element.draggable({
          appendTo: "body",
          cursorAt: {
            top: 0,
            left: 0
          },
          helper: function(event) {
            var dragHelperClass;
            dragHelperClass = $element.attr("drag-helper-class");
            return $("<div class=\"" + dragHelperClass + "\"></div>");
          }
        });
      }
    };
  });

  module.directive("multidrop", function() {
    return {
      restrict: 'A',
      priority: 2,
      link: function(scope, element, attrs) {
        var $element, droppedNodes, options, optionsAttr, _ref;
        $element = $(element);
        optionsAttr = (_ref = $element.attr("multidrop")) != null ? _ref : {};
        options = scope.$eval(optionsAttr);
        droppedNodes = function(ui) {
          var $alsoSelectedNodes, $droppedNode, $droppedNodes, node;
          $droppedNode = ui.draggable;
          $droppedNodes = $([$droppedNode]);
          if (options.isBeingDroppedSelector != null) {
            $alsoSelectedNodes = (function() {
              var _i, _len, _ref1, _results;
              _ref1 = $droppedNode.siblings("" + options.isBeingDroppedSelector);
              _results = [];
              for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
                node = _ref1[_i];
                _results.push($(node));
              }
              return _results;
            })();
            $droppedNodes = $.merge($droppedNodes, $alsoSelectedNodes);
          }
          return $droppedNodes;
        };
        return $element.droppable({
          over: function(event, ui) {
            return $element.addClass('hover');
          },
          out: function(event, ui) {
            return $element.removeClass('hover');
          },
          drop: function(event, ui) {
            var $droppedNodes, callback, callbackArgs, droppedObjects, node;
            if (options.onDrop == null) {
              return;
            }
            $droppedNodes = droppedNodes(ui);
            droppedObjects = (function() {
              var _i, _len, _results;
              _results = [];
              for (_i = 0, _len = $droppedNodes.length; _i < _len; _i++) {
                node = $droppedNodes[_i];
                _results.push(node.data('multidrop-object'));
              }
              return _results;
            })();
            if (!droppedObjects) {
              droppedObjects = $droppedNodes;
            }
            callback = scope[options.onDrop];
            callbackArgs = [droppedObjects];
            return scope.$apply(callback.apply(scope, callbackArgs));
          },
          deactivate: function(event, ui) {
            if (scope.excludedFilesCount > 0) {
              return $element.addClass('drop');
            } else {
              return $element.removeClass('drop');
            }
          }
        });
      }
    };
  });

  module.directive("multidropModel", function() {
    return {
      restrict: 'A',
      link: function(scope, element, attrs) {
        var $element, attachedObject, attachedObjectAttr;
        $element = $(element);
        attachedObjectAttr = $element.attr("multidrop-model");
        attachedObject = scope.$eval(attachedObjectAttr);
        return $element.data('multidrop-object', attachedObject);
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('swing.directives.table');

  module.directive("tableSelect", function($parse) {
    return {
      restrict: 'A',
      link: function(scope, element, attrs) {
        return $(element).tableSelect();
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('swing.directives.table');

  module.directive("reorderable", function($parse) {
    return {
      restrict: 'A',
      link: function(scope, element, attrs) {
        var reorderableModel;
        reorderableModel = $parse(attrs.reorderable);
        return element.reorderabletable({
          stop: function(newOrder) {
            var _this = this;
            return scope.$apply(function() {
              return reorderableModel(scope)(newOrder);
            });
          }
        });
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('swing.directives.table');

  module.directive("showHeaderOnHover", function($parse) {
    return {
      restrict: 'A',
      link: function(scope, element, attrs) {
        var cachedColumnOuterWidths, cachedColumnWidths, createFauxTable, destroyFauxTable, fauxHoveredRow, fauxTable;
        fauxTable = null;
        fauxHoveredRow = null;
        cachedColumnWidths = [];
        cachedColumnOuterWidths = [];
        createFauxTable = function($td, $tr, e) {
          var cellWidth, cellsInRangeOuterWidths, cellsInRangeWidths, cellsToTheLeftOuterWidths, fauxHeader, fauxHeaderTH, fauxHoveredRowTD, firstContinousVisibleRange, header, headerCells, idx, left, rowCells, th, trOffset, visibleCellsFound, visibleRange, width, _i, _j, _ref, _ref1;
          trOffset = $tr.offset();
          firstContinousVisibleRange = function($elements) {
            var $el, $window, el, end, i, inRange, isVisible, scrollLeft, scrollTop, start, windowHeight, windowWidth, _i, _len;
            $window = $(window);
            scrollLeft = $window.scrollLeft();
            scrollTop = $window.scrollTop();
            windowWidth = $window.width();
            windowHeight = $window.height();
            i = 0;
            start = 0;
            end = 0;
            inRange = false;
            for (_i = 0, _len = $elements.length; _i < _len; _i++) {
              el = $elements[_i];
              $el = $(el);
              isVisible = $el.isOnScreen(scrollTop, scrollLeft, windowWidth, windowHeight);
              if (isVisible) {
                if (inRange) {
                  end = i + 1;
                } else if ($el.is(':visible')) {
                  start = i;
                  inRange = true;
                }
              } else {
                if (inRange) {
                  end = i;
                  if ($el.is(':visible')) {
                    break;
                  }
                }
              }
              i += 1;
            }
            return {
              start: start,
              end: end
            };
          };
          if (fauxTable == null) {
            cachedColumnWidths = [];
            cachedColumnOuterWidths = [];
            $tr.children().each(function(i, e) {
              var $e;
              $e = $(e);
              if ($e.is(':visible')) {
                cachedColumnWidths.push($e.width());
                return cachedColumnOuterWidths.push($e.outerWidth());
              } else {
                cachedColumnWidths.push(0);
                return cachedColumnOuterWidths.push(0);
              }
            });
            fauxTable = $('<table></table>');
            fauxTable.addClass('header-hover-table');
            fauxTable.addClass(element.attr('class'));
            fauxTable.css({
              position: "absolute"
            });
            $("body").append(fauxTable);
          }
          fauxTable.empty();
          header = element.find("tr").first();
          headerCells = $(header).children();
          rowCells = $tr.children();
          visibleRange = firstContinousVisibleRange(rowCells);
          if (cachedColumnWidths) {
            cellsInRangeWidths = cachedColumnWidths.slice(visibleRange.start, visibleRange.end);
            cellsInRangeOuterWidths = cachedColumnOuterWidths.slice(visibleRange.start, visibleRange.end);
            cellsToTheLeftOuterWidths = [];
            if (visibleRange.start) {
              cellsToTheLeftOuterWidths = cachedColumnOuterWidths.slice(0, visibleRange.start);
            }
          } else {
            cellsInRangeWidths = [];
            cellsInRangeOuterWidths = [];
            cellsToTheLeftOuterWidths = [];
          }
          left = cellsToTheLeftOuterWidths.sum();
          width = cellsInRangeOuterWidths.sum();
          fauxTable.css({
            top: trOffset.top,
            left: left,
            width: width
          });
          fauxHeader = $("<tr class=\"faux-header-black\"></tr>");
          fauxHoveredRow = $("<tr></tr>");
          fauxHeaderTH = $(headerCells.slice(visibleRange.start, visibleRange.end)).clone();
          fauxHoveredRowTD = $(rowCells.slice(visibleRange.start, visibleRange.end)).clone();
          fauxHeader.append(fauxHeaderTH);
          fauxHoveredRow.append(fauxHoveredRowTD);
          if (fauxHeaderTH.length) {
            visibleCellsFound = 0;
            for (idx = _i = _ref = fauxHeaderTH.length - 1; _ref <= 0 ? _i <= 0 : _i >= 0; idx = _ref <= 0 ? ++_i : --_i) {
              cellWidth = cellsInRangeWidths[idx];
              th = fauxHeaderTH[idx];
              $(th).width(cellWidth);
              if (visibleCellsFound < 2 && headerCells[visibleRange.start + idx].clientWidth) {
                if (visibleCellsFound === 1) {
                  $(th).addClass('penultimate');
                }
                visibleCellsFound += 1;
              }
            }
            for (idx = _j = 1, _ref1 = fauxHeaderTH.length; 1 <= _ref1 ? _j < _ref1 : _j > _ref1; idx = 1 <= _ref1 ? ++_j : --_j) {
              th = fauxHeaderTH[idx];
              if (headerCells[visibleRange.start + idx].clientWidth) {
                $(th).addClass('first');
                break;
              }
            }
          }
          fauxTable.append(fauxHeader);
          return fauxTable.append(fauxHoveredRow);
        };
        destroyFauxTable = function() {
          if (fauxTable) {
            fauxTable.detach();
          }
          return fauxTable = null;
        };
        element.on('mouseover', 'td', function(e) {
          var $td, $tr;
          $td = $(this);
          $tr = $($td.parent());
          if ($tr.data("type") === "data") {
            return createFauxTable($td, $tr, e);
          } else {
            return destroyFauxTable();
          }
        });
        element.bind('mouseout', function(e) {
          return destroyFauxTable();
        });
        element.bind('didReload', function(e) {
          return destroyFauxTable();
        });
        element.on('$destroy', function(e) {
          element.unbind('mouseout');
          element.unbind('didReload');
          return element.off('mouseover', 'td');
        });
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('swing.directives.textfield');

  module.directive("blurOnEnter", function() {
    return {
      restrict: 'A',
      link: function(scope, element, attrs) {
        var jqElement;
        if (attrs.type === 'radio' || attrs.type === 'checkbox') {
          return;
        }
        jqElement = $(element);
        return element.bind('keydown', function(e) {
          if (e.which === 13) {
            return jqElement.blur();
          }
        });
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.directives');

  module.directive("adjustHeight", function($window, $timeout) {
    return {
      link: function(scope, element, attrs) {
        var elementOffset, footerSize, onResize, w;
        w = angular.element($window);
        elementOffset = element[0].getBoundingClientRect().top;
        footerSize = parseInt(attrs["adjustHeight"], 10);
        if (isNaN(footerSize)) {
          footerSize = 68;
        }
        onResize = function() {
          var windowHeight;
          windowHeight = w.height();
          element.css({
            height: (windowHeight - elementOffset - footerSize) + "px"
          });
          return element.trigger("AdjustHeightFired");
        };
        w.bind("resize", onResize);
        onResize();
        return $timeout(onResize, 250);
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module("h2o.directives");

  module.directive("tabs", function() {
    return {
      link: function(scope, element, attrs, ctrl) {
        $('> nav a', element).on('click', function(e) {
          e.preventDefault();
          if (!$(this).parent().hasClass('open')) {
            $('> .content > .tab', element).removeClass('open');
            $('> nav li', element).removeClass('open');
            $(this).parent().addClass('open');
            $($(this).attr('href')).addClass('open');
            $('input[type=text]', $(this).attr('href')).focus();
          }
        });
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('swing.directives');

  module.directive("dialog", function($log, $rootScope, DialogService) {
    return {
      restrict: "E",
      scope: {
        id: "=",
        height: "@"
      },
      transclude: true,
      template: '<div class="dialog"><div class="dialog-overlay"></div><div class="dialog-wrapper"><div class="dialog-positioner" ng-transclude></div></div></div>',
      replace: true,
      link: function($scope, element, $attrs, listErrorsCtrl) {
        var $overlay, $wrapper;
        $wrapper = $(element.find(".dialog-wrapper"));
        $overlay = $(element.find(".dialog-overlay"));
        $wrapper.css({
          height: $scope.height
        });
        $rootScope.$on("DialogOpened", function(event, dialogID) {
          if ($scope.id === dialogID) {
            return element.addClass('open');
          }
        });
        $rootScope.$on("DialogClosed", function(event, dialogID) {
          if ($scope.id === dialogID) {
            return element.removeClass('open');
          }
        });
        $overlay.bind('click', function() {
          return DialogService.close($scope.id);
        });
        return $scope.$on("$destroy", function() {
          return element.unbind('click');
        });
      }
    };
  });

}).call(this);

(function() {
  var module, overlayDirectiveFactory,
    __indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

  module = angular.module('swing.directives.errorAndStatus');

  module.directive("jqueryNoteErrors", function($log, $rootScope, ErrorAndStatusService) {
    return {
      restrict: "A",
      terminal: true,
      require: "listerrors",
      link: function($scope, element, $attrs, listErrorsCtrl) {
        var addErrorPopup, calculateDelta, errorPopupIDToUUIDMap, errorUUIDToPopupMap, errorsDidUpdate, options, optionsAttr, removeError, removeErrorAndPopupByUUID, updateErrorPopup, _ref;
        optionsAttr = $attrs.jqueryNoteErrors;
        options = (_ref = $scope.$eval(optionsAttr)) != null ? _ref : {};
        if (options.callback == null) {
          options.callback = {};
        }
        options.callback.afterClose = function() {
          var errorUUID;
          errorUUID = errorPopupIDToUUIDMap[this.options.id];
          return removeErrorAndPopupByUUID(errorUUID);
        };
        errorUUIDToPopupMap = {};
        errorPopupIDToUUIDMap = {};
        addErrorPopup = function(error) {
          var popup;
          popup = noty(angular.copy(options));
          errorUUIDToPopupMap[error.uuid] = popup;
          errorPopupIDToUUIDMap[popup.options.id] = error.uuid;
          return updateErrorPopup(error);
        };
        updateErrorPopup = function(error) {
          var popup;
          popup = errorUUIDToPopupMap[error.uuid];
          return popup.setText(error.message);
        };
        removeError = function(errorUUID) {
          return ErrorAndStatusService.removeErrorsByUUIDs([errorUUID]);
        };
        removeErrorAndPopupByUUID = function(errorUUID) {
          var popup, popupID;
          popup = errorUUIDToPopupMap[errorUUID];
          if (popup != null) {
            popup.close();
            delete errorUUIDToPopupMap[errorUUID];
          }
          if ((popup != null) && (popup.options != null) && (popup.options.id != null)) {
            popupID = popup.options.id;
            if (__indexOf.call(errorPopupIDToUUIDMap, popupID) >= 0) {
              delete errorPopupIDToUUIDMap[popup.options.id];
            }
          }
          if ($scope.$$phase || $scope.$root.$$phase) {
            removeError(errorUUID);
          } else {
            $scope.$apply(function() {
              return removeError(errorUUID);
            });
          }
        };
        calculateDelta = function(newErrors) {
          var error, errorPopup, errorUUID, isAdded, isRemoved, isUpdated, newError, newErrorsUUIDs, result;
          newErrorsUUIDs = (function() {
            var _i, _len, _results;
            _results = [];
            for (_i = 0, _len = newErrors.length; _i < _len; _i++) {
              newError = newErrors[_i];
              _results.push(newError.uuid);
            }
            return _results;
          })();
          isAdded = function(error) {
            var _ref1;
            return _ref1 = error.uuid, __indexOf.call(errorUUIDToPopupMap, _ref1) < 0;
          };
          isUpdated = function(error) {
            var _ref1;
            return (_ref1 = error.uuid, __indexOf.call(errorUUIDToPopupMap, _ref1) >= 0) && !angular.equals(error, errorUUIDToPopupMap[error.uuid]);
          };
          isRemoved = function(errorUUID) {
            return __indexOf.call(newErrorsUUIDs, errorUUID) < 0;
          };
          result = {
            added: (function() {
              var _i, _len, _results;
              _results = [];
              for (_i = 0, _len = newErrors.length; _i < _len; _i++) {
                error = newErrors[_i];
                if (isAdded(error)) {
                  _results.push(error);
                }
              }
              return _results;
            })(),
            updated: (function() {
              var _i, _len, _results;
              _results = [];
              for (_i = 0, _len = newErrors.length; _i < _len; _i++) {
                error = newErrors[_i];
                if (isUpdated(error)) {
                  _results.push(error);
                }
              }
              return _results;
            })(),
            removed: (function() {
              var _results;
              _results = [];
              for (errorUUID in errorUUIDToPopupMap) {
                errorPopup = errorUUIDToPopupMap[errorUUID];
                if (isRemoved(errorUUID)) {
                  _results.push(errorUUID);
                }
              }
              return _results;
            })()
          };
          return result;
        };
        errorsDidUpdate = function() {
          var delta, error, errorUUID, _i, _j, _k, _len, _len1, _len2, _ref1, _ref2, _ref3, _results;
          delta = calculateDelta(listErrorsCtrl.errors);
          _ref1 = delta.added;
          for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
            error = _ref1[_i];
            addErrorPopup(error);
          }
          _ref2 = delta.updated;
          for (_j = 0, _len1 = _ref2.length; _j < _len1; _j++) {
            error = _ref2[_j];
            updateErrorPopup(error);
          }
          _ref3 = delta.removed;
          _results = [];
          for (_k = 0, _len2 = _ref3.length; _k < _len2; _k++) {
            errorUUID = _ref3[_k];
            _results.push(removeErrorAndPopupByUUID(errorUUID));
          }
          return _results;
        };
        $rootScope.$watch((function() {
          return listErrorsCtrl.errors;
        }), errorsDidUpdate, true);
        return errorsDidUpdate();
      }
    };
  });

  module.directive("listerrors", function($log, $rootScope, ErrorAndStatusService) {
    return {
      restrict: "E",
      scope: true,
      controller: function($scope, $attrs, $element) {
        var filter, maxShown, show, to, _ref, _ref1, _ref2,
          _this = this;
        to = $attrs.to;
        filter = (_ref = $scope.$eval($attrs.filter)) != null ? _ref : {};
        show = (_ref1 = $scope.$eval($attrs.show)) != null ? _ref1 : {};
        maxShown = (_ref2 = show.max) != null ? _ref2 : 0;
        this.errors = [];
        this.updateErrors = function() {
          _this.errors = ErrorAndStatusService.errorsThatFitPattern(filter);
          if ((maxShown != null) && maxShown > 0 && _this.errors.length > maxShown) {
            _this.errors = _this.errors.slice(0, maxShown);
          }
          if (to != null) {
            return $scope[to] = _this.errors;
          }
        };
        return this;
      },
      link: function($scope, element, $attrs, listErrorsCtrl) {
        $rootScope.$on(ErrorAndStatusServiceErrorsChangedEvent, listErrorsCtrl.updateErrors);
        return listErrorsCtrl.updateErrors();
      }
    };
  });

  overlayDirectiveFactory = function(decorateOverlayWrapperFn) {
    var $template, $wrapperTemplate;
    $wrapperTemplate = decorateOverlayWrapperFn($('<div class="wrapper"></div></div>'));
    $template = $('<div class="positioner"></div>').append($wrapperTemplate);
    return function($log) {
      return {
        restrict: "E",
        template: $template[0].outerHTML,
        transclude: true,
        scope: false,
        link: function(scope, element, attrs) {
          var $children, $element, $parent, $positioner, $wrapper, alignment, color, currentParentPosition, edgeMargin, height, leftOffset, options, optionsAttr, size, topOffset, width, _ref, _ref1, _ref2, _ref3, _ref4, _ref5, _ref6, _ref7, _ref8;
          $element = $(element).addClass("overlay");
          $parent = $($element.parent());
          $positioner = $element.find(".positioner");
          $wrapper = $element.find(".wrapper");
          $children = $($wrapper.children());
          optionsAttr = attrs.options;
          options = (_ref = scope.$eval(optionsAttr)) != null ? _ref : {};
          alignment = (_ref1 = options.alignment) != null ? _ref1 : "center";
          edgeMargin = (_ref2 = options.edgeMargin) != null ? _ref2 : "5px";
          color = (_ref3 = options.color) != null ? _ref3 : "white";
          size = (_ref4 = options.size) != null ? _ref4 : "15px";
          topOffset = (_ref5 = options.topOffset) != null ? _ref5 : "0px";
          width = (_ref6 = options.width) != null ? _ref6 : "100%";
          height = (_ref7 = options.height) != null ? _ref7 : "100%";
          leftOffset = (_ref8 = options.leftOffset) != null ? _ref8 : "0px";
          if (alignment === "top" || alignment === "bottom") {
            alignment = "center " + alignment;
          }
          if (alignment === "left" || alignment === "right") {
            alignment = "" + alignment + " center";
          }
          currentParentPosition = $parent.css("position");
          if ((currentParentPosition == null) || currentParentPosition === "static") {
            $parent.css({
              position: "relative"
            });
          } else if (currentParentPosition !== "absolute" && currentParentPosition !== "relative") {
            $log.error("<spinneroverlayparent />'s parent has 'position' style set to a value in which it cannot work properly ('" + currentParentPosition + "').");
          }
          $element.css({
            position: "absolute",
            top: topOffset,
            left: leftOffset,
            width: width,
            height: height,
            "z-index": 1000
          });
          $positioner.css({
            position: "absolute"
          });
          $wrapper.css({
            width: size,
            height: size
          });
          $children.css({
            position: "relative",
            display: "block",
            "margin-top": "-50%",
            "font-family": 'FontAwesome',
            "color": color,
            "font-size": size,
            "width": size,
            "height": size
          });
          if (alignment === "left center" || alignment === "center" || alignment === "right center") {
            $positioner.css({
              top: "50%"
            });
          }
          if (alignment === "center top" || alignment === "center" || alignment === "center bottom") {
            $positioner.css({
              left: "50%"
            });
            $children.css({
              left: "-50%",
              "text-align": "center"
            });
          }
          if (alignment === "left top" || alignment === "left center" || alignment === "left bottom") {
            $children.css({
              left: "-50%"
            });
            $positioner.css({
              left: edgeMargin
            });
          }
          if (alignment === "right top" || alignment === "right center" || alignment === "right bottom") {
            $children.css({
              right: "-50%"
            });
            $positioner.css({
              right: edgeMargin
            });
          }
          if (alignment === "left top" || alignment === "center top" || alignment === "right top") {
            $positioner.css({
              top: edgeMargin
            });
          }
          if (alignment === "left bottom" || alignment === "center bottom" || alignment === "right bottom") {
            $positioner.css({
              bottom: edgeMargin
            });
          }
        }
      };
    };
  };

  module.directive("overlay", overlayDirectiveFactory(function($wrapper) {
    return $wrapper.attr("ng-transclude", "");
  }));

  module.directive("spinneroverlay", overlayDirectiveFactory(function($wrapper) {
    return $wrapper.html("<i class=\"icon-spinner icon-spin\"></i>");
  }));

}).call(this);

(function() {
  var fauxNode, module, setStyleAttributeFunc;

  module = angular.module('swing.fastNg', []);

  setStyleAttributeFunc = function(element, value) {
    return element.setAttribute('style', value);
  };

  fauxNode = $("<div></div>")[0];

  if (fauxNode.style && typeof fauxNode.style.cssText === 'string') {
    setStyleAttributeFunc = function(element, value) {
      return element.style.cssText = value;
    };
  }

  module.directive("fastNgStyle", function($parse, $rootScope) {
    return {
      link: function(scope, element, attr) {
        return scope.$watch(attr.fastNgStyle, function(newStyles, oldStyles) {
          var k, styleString, v;
          styleString = "";
          for (k in newStyles) {
            v = newStyles[k];
            styleString += "" + k + ":" + v + ";";
          }
          return setStyleAttributeFunc(element[0], styleString);
        }, true);
      }
    };
  });

  module.directive("fastNgClass", function($parse, $rootScope) {
    return {
      restrict: 'AC',
      link: function(scope, element, attr) {
        var originalClasses;
        originalClasses = element[0].getAttribute('class');
        return scope.$watch(attr.fastNgClass, function(newClasses, oldClasses) {
          var classString, isSet, k;
          classString = originalClasses + "";
          for (k in newClasses) {
            isSet = newClasses[k];
            if (isSet) {
              classString += " " + k;
            }
          }
          return element[0].setAttribute('class', classString);
        }, true);
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.directives.parse');

  module.directive("progressWave", function(ParseJobStatusDataService) {
    return {
      restrict: 'A',
      transclude: true,
      link: function(scope, element, attrs) {
        var amount, height, i, path;
        paper.install(window);
        paper.setup(element[0]);
        amount = 7;
        height = 12;
        path = new Path({
          fillColor: '#f2df0a'
        });
        i = 0;
        while (i < amount) {
          path.add(new Point((i / amount) * view.bounds.width, view.bounds.height));
          i++;
        }
        path.add(new Point(view.bounds.width + (view.bounds.width / amount), view.bounds.height));
        path.add(paper.view.bounds.bottomRight);
        path.add(paper.view.bounds.bottomLeft);
        path.add(new Point(-(view.bounds.width / amount), view.bounds.height));
        path.selected = false;
        path.closed = true;
        view.onFrame = function(event) {
          var segment, sinus;
          i = 0;
          while (i <= amount) {
            segment = path.segments[i];
            sinus = Math.sin(event.time * 3 + (i / (amount / 15)));
            if (segment.point.y !== view.bounds.height - (view.bounds.height * (ParseJobStatusDataService.jobStatus.progressPercent() / 100)) - (sinus * height)) {
              if (segment.point.y < view.bounds.height - (view.bounds.height * (ParseJobStatusDataService.jobStatus.progressPercent() / 100)) - (sinus * height)) {
                segment.point.y += 0.25;
              } else {
                segment.point.y -= 0.25;
              }
            }
            i++;
          }
        };
        path.smooth();
        paper.view.draw();
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('swing.filters');

  module.filter("humanizeFilesize", function() {
    return function(filesize) {
      return humanize.filesize(filesize);
    };
  });

  module.filter("humanizeRelativeTimeFromTimestamp", function() {
    return function(timestampInMiliseconds) {
      var timestampInSeconds;
      timestampInSeconds = timestampInMiliseconds / 1000.0;
      return humanize.relativeTime(timestampInSeconds);
    };
  });

  module.filter("roundFloat", function() {
    return function(float, decimalPlaces) {
      if (decimalPlaces == null) {
        decimalPlaces = 1;
      }
      return float.toFixed(decimalPlaces);
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('swing.filters');

  module.filter("minZero", function() {
    return function(input) {
      if (input < 0) {
        return 0;
      } else {
        return input;
      }
    };
  });

}).call(this);

(function() {
  var module;

  module = angular.module('swing.filters');

  module.filter("pathEllipsis", function() {
    return function(input, chars) {
      var output, tooLongBy;
      if (chars == null) {
        chars = 25;
      }
      if (input == null) {
        input = "";
      }
      if (chars < 3) {
        chars = 3;
      }
      if (input.length > chars) {
        tooLongBy = input.length - (chars - 3);
        output = "..." + (input.substring(tooLongBy));
      } else {
        output = input;
      }
      return output;
    };
  });

  module.filter("lastPathComponent", function() {
    return function(path) {
      var pathComponents;
      if (path == null) {
        path = "";
      }
      pathComponents = path.split('/');
      return pathComponents[pathComponents.length - 1];
    };
  });

}).call(this);

(function() {
  var module;

  this.HTTP_STATUS = {
    "ok": 0,
    "loading": 1,
    "error": 2
  };

  module = angular.module('h2o.services.inspect');

  module.service('InspectDataService', function($http, $log, $rootScope) {
    var _this = this;
    this.offset = 0;
    this.defaultLimit = 100;
    this.maxLimit = 1000;
    this.limit = this.defaultLimit;
    this.data = {};
    this.columns = [];
    this.numCols = 0;
    this.numRows = 0;
    this.meta = {
      key: "",
      rowSize: 0,
      valueSizeBytes: 0,
      processingTime: 0
    };
    this.error = "";
    this.status = HTTP_STATUS.ok;
    this.isLoading = false;
    this.validateLimit = function() {
      var intLimitValue, isIntLimitValueInvalid;
      intLimitValue = parseInt(_this.limit);
      isIntLimitValueInvalid = isNaN(intLimitValue) || intLimitValue <= 0;
      if (isIntLimitValueInvalid) {
        return _this.limit = _this.defaultLimit;
      } else {
        if (intLimitValue > _this.maxLimit) {
          return _this.limit = _this.maxLimit;
        } else {
          return _this.limit = intLimitValue;
        }
      }
    };
    this.setStatus = function(status) {
      _this.status = status;
      if (status === HTTP_STATUS.ok) {
        _this.isLoading = false;
        _this.error = "";
      }
      if (status === HTTP_STATUS.loading) {
        _this.isLoading = true;
        _this.error = "";
      }
      if (status === HTTP_STATUS.error) {
        return _this.isLoading = false;
      }
    };
    this.apiURI = function() {
      return JSONApiServerURI();
    };
    this.apiFetchURIString = function() {
      return _this.apiURI().addQuery({
        offset: _this.offset,
        view: _this.limit != null ? _this.limit : void 0
      }).toString();
    };
    this.checkColumnValidity = function(column, data) {
      if (column.base == null) {
        $log.error("'base' missing from column", column);
        return false;
      }
      if (column.max == null) {
        $log.error("'max' missing from column", column);
        return false;
      }
      if (column.min == null) {
        $log.error("'min' missing from column", column);
        return false;
      }
      if (column.mean == null) {
        $log.error("'mean' missing from column", column);
        return false;
      }
      if (column.num_missing_values == null) {
        $log.error("'num_missing_values' missing from column", column);
        return false;
      }
      if (column.name == null) {
        $log.error("'name' missing from column", column);
        return false;
      }
      if (column.variance == null) {
        $log.error("'name' missing from column", column);
        return false;
      }
      if (column.type == null) {
        $log.error("'type' missing from column", column);
        return false;
      }
      return true;
    };
    this.checkRowValidity = function(row, data) {
      if (row.row == null) {
        $log.error("'row' missing from row", row);
        return false;
      }
      return true;
    };
    this.checkResponseValidity = function(data) {
      var colsValid, rowsValid;
      if (data.key == null) {
        $log.warn("'key' missing from response");
        return false;
      }
      if (data.row_size == null) {
        $log.error("'row_size' missing from response");
        return false;
      }
      if (data.value_size_bytes == null) {
        $log.error("'value_size_bytes' missing from response");
        return false;
      }
      if (data.num_rows == null) {
        $log.error("'num_rows' missing from response");
        return false;
      }
      if (data.num_cols == null) {
        $log.error("'num_cols' missing from response");
        return false;
      }
      if (data.cols == null) {
        $log.error("'cols' missing from response");
        return false;
      }
      if (data.rows == null) {
        $log.error("'rows' missing from response");
        return false;
      }
      if (!data.status === "done") {
        $log.error("Wrong status: ", data.status);
        return false;
      }
      colsValid = true;
      angular.forEach(data.cols, function(c) {
        if (!_this.checkColumnValidity(c, data)) {
          return colsValid = false;
        }
      });
      if (!colsValid) {
        $log.error("Columns not valid: ", data.cols);
        return false;
      }
      rowsValid = true;
      angular.forEach(data.rows, function(r) {
        if (!_this.checkRowValidity(r, data)) {
          return rowsValid = false;
        }
      });
      if (!rowsValid) {
        $log.error("Rows not valid: ", data.cols);
        return false;
      }
      return true;
    };
    this.fetch = function(newOffset) {
      _this.setStatus(HTTP_STATUS.loading);
      return $http({
        method: "GET",
        url: _this.apiFetchURIString()
      }).success(function(data, status, headers, config) {
        var newColumns, newRows, _ref, _ref1;
        if (!_this.checkResponseValidity(data)) {
          _this.error = (_ref = data.error) != null ? _ref : 'Received data is not valid';
          _this.setStatus(HTTP_STATUS.error);
          return;
        }
        newColumns = [];
        newColumns.push({
          name: "row",
          visual: "Row",
          unfilterable: true
        });
        angular.forEach(data.cols, function(c) {
          c.visual = c.name;
          return newColumns.push(c);
        });
        newRows = [];
        angular.forEach(data.rows, function(r) {
          return newRows.push(r);
        });
        _this.numRows = data.num_rows;
        _this.numCols = data.num_cols;
        _this.rows = newRows;
        _this.columns = newColumns;
        _this.limit = _this.rows.length > 0 ? _this.rows.length : null;
        _this.meta = {
          key: data.key,
          rowSize: data.row_size,
          valueSizeBytes: data.value_size_bytes,
          processingTime: (_ref1 = data.response.time) != null ? _ref1 : 0
        };
        _this.setStatus(HTTP_STATUS.ok);
        return $log.log('Done', _this.numRows, _this.numCols, data);
      }).error(function(data, status, headers, config) {
        $log.log('Error');
        _this.error = "Could not communicate with the backend.";
        return _this.setStatus(HTTP_STATUS.error);
      });
    };
  });

  module.service('InspectColumnService', function($http, $log, $rootScope, InspectDataService) {
    var _this = this;
    this.tableData = [];
    this.tableHeaders = [];
    this.filterableTableHeaders = [];
    this.orderedTableHeaders = [];
    this.shownTableHeaders = {};
    this.filteredTableHeaders = [];
    this.columnsShown = 0;
    this.defaultOrder = [];
    this.init = function() {
      var $scope, prepareDataFunc;
      prepareDataFunc = function(newVal, oldVal, scope) {
        if (newVal == null) {
          return;
        }
        return _this.prepareData();
      };
      $scope = $rootScope.$new();
      $scope.$watch((function() {
        return InspectDataService.rows;
      }), prepareDataFunc);
      $scope.$watch((function() {
        return InspectDataService.columns;
      }), prepareDataFunc);
      return _this;
    };
    this.columnForColumnName = function(cName) {
      var column, _i, _len, _ref;
      _ref = this.tableHeaders;
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        column = _ref[_i];
        if (column.name === cName) {
          return column;
        }
      }
      return null;
    };
    this.prepareData = function() {
      var newDefaultOrder, newFilterableTableHeaders, newTableData, newTableHeaders;
      newTableData = [];
      newDefaultOrder = [];
      newTableHeaders = [];
      newFilterableTableHeaders = [];
      angular.forEach(InspectDataService.columns, function(c) {
        var _ref;
        newTableHeaders.push(c);
        newDefaultOrder.push(c);
        if (!((_ref = c.unfilterable) != null ? _ref : false)) {
          return newFilterableTableHeaders.push(c);
        }
      });
      angular.forEach(InspectDataService.rows, function(r) {
        return newTableData.push(r);
      });
      _this.defaultOrder = newDefaultOrder;
      _this.tableData = newTableData;
      _this.tableHeaders = newTableHeaders;
      _this.filterableTableHeaders = newFilterableTableHeaders;
      return _this.refilter();
    };
    this.resetFiltering = function() {
      angular.forEach(_this.tableHeaders, function(c) {
        return _this.shownTableHeaders[c.name] = true;
      });
      return _this.orderedTableHeaders = _this.tableHeaders;
    };
    this.revertToDefaultOrder = function() {
      return _this.setNewColumnOrder(_this.defaultOrder);
    };
    this.setNewColumnOrder = function(newOrderedTableHeaders) {
      _this.orderedTableHeaders = newOrderedTableHeaders;
      return _this.refilter();
    };
    this.setNewColumnOrderByNames = function(newOrder) {
      var newOrderedTableHeaders;
      newOrderedTableHeaders = [];
      angular.forEach(newOrder, function(cName) {
        return newOrderedTableHeaders.push(_this.columnForColumnName(cName));
      });
      return _this.setNewColumnOrder(newOrderedTableHeaders);
    };
    this.refilter = function() {
      var newColumnsShown, newFilteredTableHeaders;
      if ((_this.orderedTableHeaders == null) || _this.orderedTableHeaders.length === 0) {
        _this.resetFiltering();
      }
      newFilteredTableHeaders = [];
      angular.forEach(_this.orderedTableHeaders, function(c) {
        return newFilteredTableHeaders.push(c);
      });
      newColumnsShown = 0;
      angular.forEach(newFilteredTableHeaders, function(c) {
        var _ref;
        c.shown = (_ref = _this.shownTableHeaders[c.name]) != null ? _ref : true;
        return newColumnsShown += c.shown;
      });
      _this.columnsShown = newColumnsShown;
      return _this.filteredTableHeaders = newFilteredTableHeaders;
    };
    return this.init();
  });

}).call(this);

(function() {
  var File, generateSeparatorList, module;

  module = angular.module('h2o.services.parse.common');

  File = (function() {
    function File(key, filesize, excluded) {
      if (filesize == null) {
        filesize = 0;
      }
      if (excluded == null) {
        excluded = false;
      }
      this.key = Model.sanitizeString(key);
      this.filesize = Model.sanitizeFloat(filesize);
      this.excluded = Model.sanitizeBool(excluded);
      return;
    }

    File.prototype.lastPathComponent = function() {
      var path, pathComponents, _ref;
      path = (_ref = this.key) != null ? _ref : "";
      pathComponents = path.split('/');
      return pathComponents[pathComponents.length - 1];
    };

    return File;

  })();

  window.File = File;

  generateSeparatorList = function() {
    var i, separatorsDicts, unwriteableSeparatorsDisplayNames, _i, _j, _ref, _ref1;
    separatorsDicts = [];
    separatorsDicts.push({
      displayName: "guessed value",
      separator: "RESET"
    });
    unwriteableSeparatorsDisplayNames = ["NULL", "SOH", "STX", "ETX", "EOT", "ENQ", "ACK", "BEL '\\a'", "BS '\b'", "HT '\\t'", "LF '\\n'", "VT '\\v'", "FF '\\f'", "CR '\\r'", "SO", "SI", "DLE", "DC1", "DC2", "DC3", "DC4", "NAK", "SYN", "ETB", "CAN", "EM", "SUB", "ESC", "FS", "GS", "RS", "US", "SPACE ' '"];
    for (i = _i = 0, _ref = unwriteableSeparatorsDisplayNames.length; 0 <= _ref ? _i <= _ref : _i >= _ref; i = 0 <= _ref ? ++_i : --_i) {
      separatorsDicts.push({
        displayName: unwriteableSeparatorsDisplayNames[i],
        separator: String.fromCharCode(i)
      });
    }
    for (i = _j = _ref1 = unwriteableSeparatorsDisplayNames.length + 1; _ref1 <= 126 ? _j <= 126 : _j >= 126; i = _ref1 <= 126 ? ++_j : --_j) {
      separatorsDicts.push({
        displayName: String.fromCharCode(i),
        separator: String.fromCharCode(i)
      });
    }
    return separatorsDicts;
  };

  module.service('ParseSettingsService', function($log, $rootScope) {
    var _this = this;
    this.SEPARATORS = generateSeparatorList();
    this.COLUMN_TYPES = [
      {
        displayName: "Enum",
        type: 'ENUM'
      }, {
        displayName: "Integer",
        type: 'INT'
      }, {
        displayName: "Float",
        type: 'DOUBLE'
      }, {
        displayName: "Auto",
        type: 'AUTO'
      }
    ];
    this.reset = function() {
      this.dst = null;
      this.initialFiles = [];
      this.files = [];
      this.additionalHeaderFiles = [];
      this.parserType = null;
      this.headerSeparator = "RESET";
      this.dataSeparator = "RESET";
      this.skipHeader = null;
      this.headerFile = null;
      this.userSetColumnNames = null;
      this.columns = null;
      this.numCols = 0;
      this.job = null;
      this.parserConfigData = null;
      this.savedContext = null;
      this.lastGoodContext = null;
      if (this.wizardID != null) {
        this.deleteFromLocalStorage(this.wizardID);
        return this.saveToLocalStorage(this.wizardID);
      }
    };
    this.init = function() {
      var onChangeSaveState, onChangeUpdateFieldsAndSaveState,
        _this = this;
      this.reset();
      onChangeUpdateFieldsAndSaveState = function() {
        return {
          'initialFiles': _this.initialFiles,
          'files': _this.files,
          'additionalHeaderFiles': _this.additionalHeaderFiles,
          'parserType': _this.parserType,
          'headerSeparator': _this.headerSeparator,
          'dataSeparator': _this.dataSeparator,
          'skipHeader': _this.skipHeader,
          'headerFile': _this.headerFile,
          'columnsDirty': _this.columnsDirty
        };
      };
      $rootScope.$watch(onChangeUpdateFieldsAndSaveState, this.updateParserConfigDataFromFields, true);
      onChangeSaveState = function() {
        return {
          'dst': _this.dst,
          'job': _this.job,
          'wizardID': _this.wizardID
        };
      };
      $rootScope.$watch(onChangeSaveState, this.saveWizardState, true);
    };
    this.receivedInitialFilesAndDst = function(files, dst) {
      this.reset();
      this.dst = dst;
      this.initialFiles = angular.copy(files);
      return this.files = files;
    };
    this.addFile = function(newFile, alsoToInitialFiles) {
      var alreadyAdded, file;
      if (alsoToInitialFiles == null) {
        alsoToInitialFiles = true;
      }
      alreadyAdded = (function() {
        var _i, _len, _ref, _results;
        _ref = this.files;
        _results = [];
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          file = _ref[_i];
          if (file.key === newFile.key) {
            _results.push(file);
          }
        }
        return _results;
      }).call(this);
      if (alreadyAdded.length === 0) {
        this.files.push(newFile);
        if (alsoToInitialFiles) {
          this.initialFiles.push(newFile);
        }
      } else {
        alreadyAdded[0].key = newFile.key;
        alreadyAdded[0].filesize = newFile.filesize;
        alreadyAdded[0].excluded = newFile.excluded;
      }
    };
    this.addAdditionalHeaderFile = function(newFile) {
      var alreadyAdded, file;
      alreadyAdded = (function() {
        var _i, _len, _ref, _results;
        _ref = this.additionalHeaderFiles;
        _results = [];
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          file = _ref[_i];
          if (file.key === newFile.key) {
            _results.push(file);
          }
        }
        return _results;
      }).call(this);
      if (alreadyAdded.length === 0) {
        this.additionalHeaderFiles.push(newFile);
      } else {
        alreadyAdded[0].key = newFile.key;
        alreadyAdded[0].filesize = newFile.filesize;
        alreadyAdded[0].excluded = newFile.excluded;
      }
    };
    this.resetToInitialFilesState = function() {
      return _this.files = angular.copy(_this.initialFiles);
    };
    this.hasFiles = function() {
      return this.files.length > 0;
    };
    this.includedFiles = function() {
      var file, _i, _len, _ref, _results;
      _ref = this.files;
      _results = [];
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        file = _ref[_i];
        if (!file.excluded) {
          _results.push(file);
        }
      }
      return _results;
    };
    this.excludedFiles = function() {
      var file, _i, _len, _ref, _results;
      _ref = this.files;
      _results = [];
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        file = _ref[_i];
        if (!file.excluded) {
          _results.push(file);
        }
      }
      return _results;
    };
    this.hasFilesIncluded = function() {
      return this.includedFiles().length > 0;
    };
    this.columnsDirty = false;
    this.didChangeColumnSettings = function() {
      return this.columnsDirty = true;
    };
    this.setParserConfigData = function(newParserConfigData) {
      _this.parserConfigData = newParserConfigData;
      _this.updateFieldsFromParserConfigData();
      return _this.saveLastGoodResponse();
    };
    this.updateParserConfigDataFilesFromFieldTo = function(parserConfigData) {
      var file;
      if (_this.files != null) {
        parserConfigData['uris'] = (function() {
          var _i, _len, _ref, _results;
          _ref = this.includedFiles();
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            file = _ref[_i];
            _results.push(file.key);
          }
          return _results;
        }).call(_this);
      }
      return parserConfigData;
    };
    this.updateFieldsFromParserConfigData = function() {
      if (_this.parserConfigData == null) {
        return;
      }
      _this.parserConfigData = _this.updateParserConfigDataFilesFromFieldTo(_this.parserConfigData);
      if (_this.parserConfigData.parser_type != null) {
        _this.parserType = _this.parserConfigData.parser_type;
      }
      if (_this.parserConfigData.header_separator != null) {
        _this.headerSeparator = _this.parserConfigData.header_separator;
      }
      if (_this.parserConfigData.data_separator != null) {
        _this.dataSeparator = _this.parserConfigData.data_separator;
      }
      if (_this.parserConfigData.user_set_column_names != null) {
        _this.userSetColumnNames = _this.parserConfigData.user_set_column_names;
      }
      if (_this.parserConfigData.dont_skip_header != null) {
        _this.skipHeader = !_this.parserConfigData.dont_skip_header;
      }
      if (_this.parserConfigData.header_file != null) {
        _this.headerFile = _this.parserConfigData.header_file;
      }
      if (_this.parserConfigData.columns != null) {
        _this.columns = _this.parserConfigData.columns;
        _this.numCols = _this.parserConfigData.columns.length;
      }
      _this.columnsDirty = false;
      return _this.saveWizardState();
    };
    this.typeIsArray = function(value) {
      return value && typeof value === 'object' && value instanceof Array && typeof value.length === 'number' && typeof value.splice === 'function' && !(value.propertyIsEnumerable('length'));
    };
    this.updateParserConfigDataFromFields = function() {
      var dirty, k, newParserConfigData, v, _ref;
      newParserConfigData = {};
      newParserConfigData = _this.updateParserConfigDataFilesFromFieldTo(newParserConfigData);
      if (_this.parserType != null) {
        newParserConfigData['parser_type'] = _this.parserType;
      }
      if ((_this.headerSeparator != null) && _this.headerSeparator !== "RESET") {
        newParserConfigData['header_separator'] = _this.headerSeparator;
      }
      if ((_this.dataSeparator != null) && _this.dataSeparator !== "RESET") {
        newParserConfigData['data_separator'] = _this.dataSeparator;
      }
      if (_this.skipHeader != null) {
        newParserConfigData['dont_skip_header'] = !_this.skipHeader;
      }
      if (_this.headerFile != null) {
        newParserConfigData['header_file'] = _this.headerFile;
      }
      if (_this.userSetColumnNames != null) {
        newParserConfigData['user_set_column_names'] = _this.userSetColumnNames;
      }
      _ref = _this.parserConfigData;
      for (k in _ref) {
        v = _ref[k];
        if (newParserConfigData[k] == null) {
          newParserConfigData[k] = v;
        }
      }
      dirty = _this.columnsDirty;
      for (k in newParserConfigData) {
        v = newParserConfigData[k];
        if (k === "additionalHeaderFiles") {
          continue;
        }
        if (k === "userSetColumnNames") {
          continue;
        }
        if ((_this.parserConfigData != null) && (_this.parserConfigData[k] != null)) {
          dirty |= JSON.stringify(newParserConfigData[k]) !== JSON.stringify(_this.parserConfigData[k]);
        } else {
          dirty = true;
        }
        if (dirty) {
          break;
        }
      }
      if (_this.columns != null) {
        newParserConfigData['columns'] = _this.columns;
      }
      if (dirty) {
        $log.debug("[ParserConfigDataChanged] event fired.");
        _this.parserConfigData = newParserConfigData;
        _this.columnsDirty = false;
        $rootScope.$broadcast("ParserConfigDataChanged");
      }
      return _this.saveWizardState();
    };
    this.saveWizardState = function() {
      if (_this.wizardID != null) {
        return _this.saveToLocalStorage(_this.wizardID);
      }
    };
    this.deleteWizardState = function() {
      if (_this.wizardID != null) {
        return _this.deleteFromLocalStorage(_this.wizardID);
      }
    };
    this.serialize = function() {
      return {
        dst: _this.dst,
        job: _this.job,
        initialFiles: angular.copy(_this.initialFiles),
        files: angular.copy(_this.files),
        additionalHeaderFiles: angular.copy(_this.additionalHeaderFiles),
        parserType: _this.parserType,
        headerSeparator: _this.headerSeparator,
        dataSeparator: _this.dataSeparator,
        skipHeader: _this.skipHeader,
        headerFile: _this.headerFile,
        userSetColumnNames: _this.userSetColumnNames,
        columns: angular.copy(_this.columns),
        numCols: _this.numCols,
        parserConfigData: angular.copy(_this.parserConfigData)
      };
    };
    this.deserialize = function(dict) {
      var fileDict;
      this.dst = dict.dst;
      this.job = dict.job;
      if (dict.files != null) {
        this.files = (function() {
          var _i, _len, _ref, _results;
          _ref = dict.files;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            fileDict = _ref[_i];
            _results.push(new File(fileDict.key, fileDict.filesize, fileDict.excluded));
          }
          return _results;
        })();
      } else {
        this.files = [];
      }
      if (dict.additionalHeaderFiles != null) {
        this.additionalHeaderFiles = (function() {
          var _i, _len, _ref, _results;
          _ref = dict.additionalHeaderFiles;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            fileDict = _ref[_i];
            _results.push(new File(fileDict.key, fileDict.filesize, fileDict.excluded));
          }
          return _results;
        })();
      } else {
        this.additionalHeaderFiles = [];
      }
      if (dict.initialFiles != null) {
        this.initialFiles = (function() {
          var _i, _len, _ref, _results;
          _ref = dict.initialFiles;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            fileDict = _ref[_i];
            _results.push(new File(fileDict.key, fileDict.filesize, fileDict.excluded));
          }
          return _results;
        })();
      } else {
        this.initialFiles = [];
      }
      this.parserType = dict.parserType;
      this.headerSeparator = dict.headerSeparator;
      this.dataSeparator = dict.dataSeparator;
      this.skipHeader = dict.skipHeader;
      this.headerFile = dict.headerFile;
      this.userSetColumnNames = dict.userSetColumnNames;
      this.columns = dict.columns;
      this.numCols = dict.numCols;
      this.parserConfigData = dict.parserConfigData;
    };
    this.deleteFromLocalStorage = function(wizardID) {
      if (typeof localStorage !== "undefined" && localStorage !== null) {
        localStorage.removeItem(wizardID);
      }
    };
    this.saveToLocalStorage = function(wizardID) {
      var e, jsonSerializedDict, serializedDict;
      if (wizardID == null) {
        return;
      }
      serializedDict = _this.serialize();
      if (typeof localStorage !== "undefined" && localStorage !== null) {
        jsonSerializedDict = JSON.stringify(serializedDict);
        try {
          localStorage.setItem(wizardID, jsonSerializedDict);
          $log.log("[ParseSettingsService.saveToLocalStorage] Saving state in key " + wizardID + ".");
        } catch (_error) {
          e = _error;
          $log.log("[ParseSettingsService.saveToLocalStorage] Quota probably exceeded.");
        }
      }
    };
    this.loadFromLocalStorage = function(wizardID) {
      var jsonSerializedDict, serializedDict;
      if (wizardID == null) {
        return;
      }
      if (typeof localStorage !== "undefined" && localStorage !== null) {
        jsonSerializedDict = localStorage.getItem(wizardID);
        if (jsonSerializedDict == null) {
          $log.log("[ParseSettingsService.loadFromLocalStorage] Key " + wizardID + " not found.");
          return false;
        }
        serializedDict = JSON.parse(jsonSerializedDict);
      }
      if (serializedDict != null) {
        this.deserialize(serializedDict);
      }
      return true;
    };
    this.saveContext = function() {
      return _this.savedContext = _this.serialize();
    };
    this.undoToPreviouslySavedContext = function() {
      this.deserialize(this.savedContext);
      return this.didChangeColumnSettings();
    };
    this.saveLastGoodResponse = function() {
      return _this.lastGoodContext = _this.serialize();
    };
    this.undoToLastGoodResponse = function() {
      if (_this.lastGoodContext == null) {
        return;
      }
      _this.deserialize(_this.lastGoodContext);
      return _this.columnsDirty = false;
    };
    this.init();
    return this;
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.services.parse');

  module.service('ParseService', function($http, $log, $q, ParseSettingsService, ErrorAndStatusService) {
    var _this = this;
    this.parseEndpoint = "/v2/parse.json";
    this.generateParseRequestData = function() {
      var result;
      result = {
        "parser_config": ParseSettingsService.parserConfigData
      };
      if (ParseSettingsService.dst != null) {
        result["dst"] = ParseSettingsService.dst;
      }
      return result;
    };
    this.validateParseResponse = function(data) {
      var message, _ref, _ref1;
      if (data.job == null) {
        message = (_ref = (data ? data.error : null)) != null ? _ref : "did not receive the job id";
        ErrorAndStatusService.setStatus('ParseService', "error");
        ErrorAndStatusService.addError('ParseService', {
          message: message
        });
        return false;
      }
      if (!ParseSettingsService.dst && (data.dst == null)) {
        message = (_ref1 = (data ? data.error : null)) != null ? _ref1 : "did not receive the destination key";
        ErrorAndStatusService.setStatus('ParseService', "error");
        ErrorAndStatusService.addError('ParseService', {
          message: message
        });
        return false;
      }
      return true;
    };
    this.didReceiveParseResponse = function(response) {
      ParseSettingsService.job = response.job;
      if (response.dst != null) {
        ParseSettingsService.dst = response.dst;
      }
    };
    this.canParse = function() {
      var _ref;
      if ((ParseSettingsService.parserConfigData == null) || ((_ref = ParseSettingsService.parserConfigData.uris) != null ? _ref : []).length === 0) {
        return false;
      }
      if (ErrorAndStatusService.isLoading('ParseService')) {
        return false;
      }
      return true;
    };
    this.startParse = function() {
      var response;
      response = $q.defer();
      if (!_this.canParse()) {
        response.reject();
        return response.promise;
      }
      ErrorAndStatusService.clearErrors({
        sender: 'ParseService'
      });
      ErrorAndStatusService.setStatus('ParseService', "loading");
      $http({
        method: "POST",
        url: _this.parseEndpoint,
        data: _this.generateParseRequestData()
      }).success(function(data, status, headers, config) {
        if (_this.validateParseResponse(data)) {
          ErrorAndStatusService.setStatus('ParseService', "ok");
          _this.didReceiveParseResponse(data);
          return response.resolve();
        } else {
          return response.reject();
        }
      }).error(function(data, status, headers, config) {
        var message, _ref;
        message = (_ref = (data ? data.error : null)) != null ? _ref : "unknown error";
        ErrorAndStatusService.setStatus('ParseService', "error");
        ErrorAndStatusService.addError('ParseService', {
          message: message
        });
        return response.reject();
      });
      return response.promise;
    };
    return this;
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.services.parse');

  module.service('ParsePreviewService', function($http, $log, $rootScope, ParseSettingsService, ErrorAndStatusService) {
    var _this = this;
    this.previewEndpoint = "/v2/parse_preview.json";
    this.requestID = null;
    this.previewLen = 10;
    this.data = null;
    this.numRows = 0;
    this.init = function() {
      return $rootScope.$on("ParserConfigDataChanged", this.parserConfigDataChanged);
    };
    this.isStarted = false;
    this.needsRefresh = false;
    this.startRefreshingPreview = function() {
      $rootScope.$broadcast("ParsePreviewDataChanged");
      this.requestID = null;
      this.isStarted = true;
      if (this.needsRefresh) {
        return this.refreshPreview();
      }
    };
    this.stopRefreshingPreview = function() {
      this.requestID = null;
      return this.isStarted = false;
    };
    this.serialize = function() {
      return {
        previewLen: angular.copy(_this.previewLen),
        data: angular.copy(_this.data),
        numRows: angular.copy(_this.numRows)
      };
    };
    this.deserialize = function(dict) {
      this.previewLen = dict.previewLen;
      this.data = dict.data;
      this.numRows = dict.numRows;
    };
    this.saveContext = function() {
      return _this.savedContext = _this.serialize();
    };
    this.undoToPreviouslySavedContext = function() {
      return this.deserialize(this.savedContext);
    };
    this.generatePreviewRequestData = function() {
      var result;
      result = {
        "parser_config": ParseSettingsService.parserConfigData
      };
      if (ParseSettingsService.dst != null) {
        result["dst"] = ParseSettingsService.dst;
      }
      if (this.previewLen != null) {
        result["preview_len"] = this.previewLen;
      }
      return result;
    };
    this.validateParserConfigData = function(data) {
      var message, _ref;
      if (data.parser_config == null) {
        message = (_ref = (data ? data.error : null)) != null ? _ref : "can't understand the server response";
        ErrorAndStatusService.setStatus('ParsePreviewService', "error");
        ErrorAndStatusService.addError('ParsePreviewService', {
          message: message
        });
        return false;
      }
      return true;
    };
    this.didReceiveParsePreviewResponse = function(response) {
      if (response.preview_len != null) {
        _this.previewLen = response.preview_len;
      }
      if (response.preview != null) {
        _this.data = response.preview;
        _this.numRows = response.preview.length;
      }
      if (response.dst != null) {
        ParseSettingsService.dst = response.dst;
      }
      ParseSettingsService.setParserConfigData(response.parser_config);
      $rootScope.$broadcast("ParsePreviewDataChanged");
    };
    this.canRefreshPreview = function() {
      var _ref;
      if ((ParseSettingsService.parserConfigData == null) || ((_ref = ParseSettingsService.parserConfigData.uris) != null ? _ref : []).length === 0) {
        return false;
      }
      if (ErrorAndStatusService.isLoading('ParsePreviewService')) {
        return false;
      }
      if (!this.isStarted) {
        this.needsRefresh = true;
        return false;
      }
      return true;
    };
    this.canRefreshPreviewAutomatically = function() {
      var columnCount, columnsInfoAvailable;
      columnsInfoAvailable = (ParseSettingsService.parserConfigData != null) && (ParseSettingsService.parserConfigData.columns != null);
      columnCount = columnsInfoAvailable ? ParseSettingsService.parserConfigData.columns.length : 0;
      if (columnCount > 1000) {
        return false;
      }
      return true;
    };
    this.parserConfigDataChanged = function() {
      $log.debug("[ParsePreviewService.parserConfigDataChanged] fired.");
      _this.requestID = uuid();
      if (_this.canRefreshPreviewAutomatically()) {
        return _this.refreshPreview();
      } else {
        return _this.needsRefresh = true;
      }
    };
    this.refreshPreview = function() {
      var requestID;
      requestID = angular.copy(_this.requestID);
      if (!_this.canRefreshPreview()) {
        return;
      }
      _this.needsRefresh = false;
      ErrorAndStatusService.clearErrors({
        sender: 'ParsePreviewService'
      });
      ErrorAndStatusService.setStatus('ParsePreviewService', "loading");
      $log.debug("Requesting new parse preview...");
      return $http({
        method: "POST",
        url: _this.previewEndpoint,
        data: _this.generatePreviewRequestData()
      }).success(function(data, status, headers, config) {
        $log.debug("Successfully refreshed parse preview.");
        if (_this.validateParserConfigData(data)) {
          ErrorAndStatusService.setStatus('ParsePreviewService', "ok");
          if (_this.requestID === requestID) {
            return _this.didReceiveParsePreviewResponse(data);
          } else {
            return _this.refreshPreview();
          }
        }
      }).error(function(data, status, headers, config) {
        var message, _ref;
        $log.debug("Error while refreshing parse preview.");
        _this.needsRefresh = true;
        message = (_ref = (data ? data.error : null)) != null ? _ref : "unknown error";
        ErrorAndStatusService.setStatus('ParsePreviewService', "error");
        return ErrorAndStatusService.addError('ParsePreviewService', {
          message: message
        });
      });
    };
    this.init();
    return this;
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.services.parse');

  module.service('ParseGetHeaderService', function($http, $log, $rootScope, $q, ParseSettingsService, ErrorAndStatusService) {
    var _this = this;
    this.getHeaderEndpoint = function(uri) {
      return "/v2/get_header?uri=" + (encodeURIComponent(uri));
    };
    this.generateRequestEndpointForFile = function(file) {
      return this.getHeaderEndpoint(file.key);
    };
    this.validateResponse = function(data, file) {
      var message, _ref, _ref1, _ref2;
      if (data.uri == null) {
        message = (_ref = (data ? data.error : null)) != null ? _ref : "did not receive the uri back";
        ErrorAndStatusService.setStatus('ParseGetHeaderService', "error");
        ErrorAndStatusService.addError('ParseGetHeaderService', {
          message: message
        });
        return false;
      }
      if (!data.uri === file.key) {
        message = (_ref1 = (data ? data.error : null)) != null ? _ref1 : "received a response with different uri for file";
        ErrorAndStatusService.setStatus('ParseGetHeaderService', "error");
        ErrorAndStatusService.addError('ParseGetHeaderService', {
          message: message
        });
        return false;
      }
      if (data.header_separator == null) {
        message = (_ref2 = (data ? data.error : null)) != null ? _ref2 : "did not receive the header separator";
        ErrorAndStatusService.setStatus('ParseGetHeaderService', "error");
        ErrorAndStatusService.addError('ParseGetHeaderService', {
          message: message
        });
        return false;
      }
      return true;
    };
    this.didReceiveResponse = function(data, file) {
      file.excluded = false;
      ParseSettingsService.addAdditionalHeaderFile(file);
      ParseSettingsService.headerFile = data.uri;
      return ParseSettingsService.headerSeparator = data.header_separator;
    };
    this.canUseFileAsHeader = function(file) {
      if (file == null) {
        return false;
      }
      if ((file.key == null) || file.key.length === 0) {
        return false;
      }
      if (ErrorAndStatusService.isLoading('ParseGetHeaderService')) {
        return false;
      }
      return true;
    };
    this.changeHeaderTo = function(file) {
      var response,
        _this = this;
      response = $q.defer();
      if (!this.canUseFileAsHeader(file)) {
        response.reject();
        return response.promise;
      }
      ErrorAndStatusService.clearErrors({
        sender: 'ParseGetHeaderService'
      });
      ErrorAndStatusService.setStatus('ParseGetHeaderService', "loading");
      $http({
        method: "GET",
        url: this.generateRequestEndpointForFile(file)
      }).success(function(data, status, headers, config) {
        if (_this.validateResponse(data, file)) {
          ErrorAndStatusService.setStatus('ParseGetHeaderService', "ok");
          _this.didReceiveResponse(data, file);
          return response.resolve();
        } else {
          return response.reject();
        }
      }).error(function(data, status, headers, config) {
        var message, _ref;
        message = (_ref = (data ? data.error : null)) != null ? _ref : "unknown error";
        ErrorAndStatusService.setStatus('ParseGetHeaderService', "error");
        ErrorAndStatusService.addError('ParseGetHeaderService', {
          message: message
        });
        return response.reject();
      });
      return response.promise;
    };
    return this;
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.services.parse');

  module.service('ParseFileUploaderService', function($http, $log, $rootScope, $q, ErrorAndStatusService) {
    var _this = this;
    this.dst = null;
    this.files = {};
    this.waitingForFiles = 0;
    this.loadUriEndpoint = function(uri, type) {
      return "/v2/list_uri.json?uri=" + (encodeURIComponent(uri)) + "&source_type=" + type;
    };
    this.postFileEndpoint = "/v2/post_file.json";
    this.reset = function() {
      ErrorAndStatusService.clearErrors({
        sender: "ParseFileUploaderService"
      });
      _this.dst = null;
      _this.files = [];
      return _this.waitingForFiles = 0;
    };
    this.loadFilesFromURI = function(uri, type) {
      var response;
      _this.reset();
      ErrorAndStatusService.setStatus("ParseFileUploaderService", "loading");
      response = $q.defer();
      $http({
        method: "GET",
        url: _this.loadUriEndpoint(uri, type)
      }).success(function(data, status, headers, config) {
        var excluded_files, file, included_files, _ref;
        ErrorAndStatusService.setStatus("ParseFileUploaderService", "ok");
        included_files = (function() {
          var _i, _len, _ref, _ref1, _results;
          _ref1 = (_ref = data.uris) != null ? _ref : [];
          _results = [];
          for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
            file = _ref1[_i];
            _results.push(new File(file.uri, file.size, false));
          }
          return _results;
        })();
        excluded_files = (function() {
          var _i, _len, _ref, _ref1, _results;
          _ref1 = (_ref = data.ignored_uris) != null ? _ref : [];
          _results = [];
          for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
            file = _ref1[_i];
            _results.push(new File(file.uri, file.size, true));
          }
          return _results;
        })();
        _this.files = included_files.concat(excluded_files);
        _this.dst = (_ref = data.dst) != null ? _ref : null;
        return response.resolve({
          files: _this.files,
          dst: _this.dst
        });
      }).error(function(data, status, headers, config) {
        var message, _ref;
        message = (_ref = (data ? data.error : null)) != null ? _ref : "unknown error";
        ErrorAndStatusService.setStatus("ParseFileUploaderService", "error");
        ErrorAndStatusService.addError("ParseFileUploaderService", {
          type: "uri",
          message: message
        });
        _this.files = [];
        return response.reject();
      });
      return response.promise;
    };
    this.waitForFiles = function(numberOfFiles) {
      _this.reset();
      ErrorAndStatusService.setStatus("ParseFileUploaderService", "loading");
      _this.waitingForFiles = numberOfFiles;
      _this.uploadFinished = $q.defer();
      return _this.uploadFinished.promise;
    };
    this.fileUploadGotResponse = function(filename, data, isError) {
      var message, _ref;
      if (isError || (data.dst == null) || (data.uri == null) || (data.size == null)) {
        message = (_ref = (data ? data.error : null)) != null ? _ref : "unknown error";
        ErrorAndStatusService.addError("ParseFileUploaderService", {
          type: "upload",
          subject: filename,
          message: message
        });
      } else {
        _this.files.push(new File(data.uri, data.size, false));
      }
      if (_this.dst == null) {
        _this.dst = data.dst;
      }
      _this.waitingForFiles -= 1;
      if (_this.waitingForFiles <= 0) {
        if (_this.files.length) {
          ErrorAndStatusService.setStatus("ParseFileUploaderService", "ok");
          return _this.uploadFinished.resolve({
            files: _this.files,
            dst: _this.dst
          });
        } else {
          ErrorAndStatusService.setStatus("ParseFileUploaderService", "error");
          return _this.uploadFinished.reject();
        }
      }
    };
    return this;
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.services.parse');

  module.service('ParseJobCancelService', function($http, $log, $rootScope, $interval, $q, ParseSettingsService, ErrorAndStatusService) {
    var _this = this;
    this.cancelEndpoint = function() {
      return "/v2/cancel_job.json?job=" + ParseSettingsService.job;
    };
    this.canCancel = function() {
      if (ErrorAndStatusService.isLoading('ParseJobCancelService')) {
        return false;
      }
      if (ParseSettingsService.job == null) {
        return false;
      }
      return true;
    };
    this.cancel = function() {
      var response;
      response = $q.defer();
      if (!_this.canCancel()) {
        response.reject();
        return response.promise;
      }
      ErrorAndStatusService.clearErrors({
        sender: 'ParseJobCancelService'
      });
      ErrorAndStatusService.setStatus('ParseJobCancelService', "loading");
      $http({
        method: "GET",
        url: _this.cancelEndpoint()
      }).success(function(data, status, headers, config) {
        ErrorAndStatusService.setStatus('ParseJobCancelService', "ok");
        return response.resolve();
      }).error(function(data, status, headers, config) {
        var message, _ref;
        message = (_ref = (data ? data.error : null)) != null ? _ref : "unknown error";
        ErrorAndStatusService.setStatus('ParseJobCancelService', "error");
        ErrorAndStatusService.addError('ParseJobCancelService', {
          message: message
        });
        return response.reject();
      });
      return response.promise;
    };
    return this;
  });

}).call(this);

(function() {
  var JobStatus, module, _ref,
    __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
    __hasProp = {}.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
    __indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

  module = angular.module('h2o.services.parse');

  JobStatus = (function(_super) {
    __extends(JobStatus, _super);

    function JobStatus() {
      this.progressPercent = __bind(this.progressPercent, this);
      _ref = JobStatus.__super__.constructor.apply(this, arguments);
      return _ref;
    }

    JobStatus.prototype.progressPercent = function() {
      return (100.0 * this.progress).toFixed(0);
    };

    JobStatus.prototype.update = function(data) {
      this.progress = Model.sanitizeFloat(data.progress);
      this.status = Model.sanitizeString(data.status);
      this.job = Model.sanitizeString(data.job);
      this.errors = Model.sanitizeArray(data.errors);
      return JobStatus.__super__.update.call(this, data);
    };

    return JobStatus;

  })(Model);

  module.service('ParseJobStatusDataService', function($http, $log, $rootScope, $interval, $q, HTTPPollerService, ParseSettingsService, ErrorAndStatusService) {
    var _this = this;
    this.jobStatus = new JobStatus();
    this.pollEndpoint = function() {
      return "/v2/parse_progress.json?job=" + (encodeURIComponent(ParseSettingsService.job));
    };
    this.init = function() {
      return this.poller = HTTPPollerService.addPoller("ParseJobStatusDataService", 1000, this.pollEndpoint, this.updateJobStatistics);
    };
    this.startPolling = function() {
      this.jobStatus.update({});
      ErrorAndStatusService.clearErrors({
        sender: 'ParseJobStatusDataService'
      });
      if (ParseSettingsService.job != null) {
        return HTTPPollerService.startPolling(this.poller);
      }
    };
    this.stopPolling = function() {
      return HTTPPollerService.stopPolling(this.poller);
    };
    this.hasErrors = function() {
      return _this.jobStatus.errors.length > 0;
    };
    this.isComplete = function() {
      return _this.jobStatus.progress >= 1;
    };
    this.isCompleteWithError = function() {
      return _this.isComplete() && _this.hasErrors();
    };
    this.updateJobStatistics = function(data, status_code, http_status) {
      var error, message, subject, _i, _len, _ref1, _ref2, _ref3;
      if (data == null) {
        return;
      }
      if (status_code !== LOADING_STATUS.ok) {
        return;
      }
      _this.jobStatus.update(data);
      ErrorAndStatusService.clearErrors({
        sender: 'ParseJobStatusDataService',
        type: "jobError"
      });
      _ref1 = _this.jobStatus.errors;
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        error = _ref1[_i];
        subject = error.file;
        message = (_ref2 = (_ref3 = error.msg) != null ? _ref3 : error) != null ? _ref2 : "Unknown error";
        if (subject != null) {
          ErrorAndStatusService.addError('ParseJobStatusDataService', {
            type: "jobError",
            subject: subject,
            message: message
          });
        } else {
          ErrorAndStatusService.addError('ParseJobStatusDataService', {
            type: "jobError",
            message: message,
            replace: false
          });
        }
      }
      if (_this.isComplete()) {
        return _this.stopPolling();
      }
    };
    this.excludeErrorneousFiles = function() {
      var distinctErrorneousFileKeys, error, errorneousFileKeys, errorsWithFilesList, file, _i, _len, _ref1, _ref2, _results;
      errorsWithFilesList = (function() {
        var _i, _len, _ref1, _results;
        _ref1 = this.jobStatus.errors;
        _results = [];
        for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
          error = _ref1[_i];
          if (error.file != null) {
            _results.push(error);
          }
        }
        return _results;
      }).call(this);
      errorneousFileKeys = (function() {
        var _i, _len, _results;
        _results = [];
        for (_i = 0, _len = errorsWithFilesList.length; _i < _len; _i++) {
          error = errorsWithFilesList[_i];
          _results.push(error.file);
        }
        return _results;
      })();
      distinctErrorneousFileKeys = jQuery.unique(errorneousFileKeys);
      _ref1 = ParseSettingsService.files;
      _results = [];
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        file = _ref1[_i];
        if (_ref2 = file.key, __indexOf.call(distinctErrorneousFileKeys, _ref2) >= 0) {
          _results.push(file.excluded = true);
        } else {
          _results.push(void 0);
        }
      }
      return _results;
    };
    this.init();
    return this;
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.services');

  module.service('MenuService', function($http, $log, $rootScope) {
    var _this = this;
    this.menus = {};
    this.groupNameFromID = function(id) {
      var components;
      components = id.split(".");
      if (components.length > 1) {
        return components[0];
      } else {
        return null;
      }
    };
    this.belongsToGroup = function(id, group) {
      return _this.groupNameFromID(id) === group;
    };
    this.IDsInGroup = function(group) {
      var id, isOpen, result, _ref;
      result = [];
      _ref = _this.menus;
      for (id in _ref) {
        isOpen = _ref[id];
        if (_this.belongsToGroup(id, group)) {
          result.push(id);
        }
      }
      return result;
    };
    this.isOpen = function(id) {
      var _ref;
      return (_ref = _this.menus[id]) != null ? _ref : false;
    };
    this.toggleOpen = function(id) {
      if (_this.isOpen(id)) {
        return _this.close(id);
      } else {
        return _this.open(id);
      }
    };
    this.open = function(id) {
      var group, otherInSameGroupID, _i, _len, _ref;
      group = _this.groupNameFromID(id);
      _ref = _this.IDsInGroup(group);
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        otherInSameGroupID = _ref[_i];
        _this.close(otherInSameGroupID);
      }
      return _this.menus[id] = true;
    };
    this.close = function(id) {
      return _this.menus[id] = false;
    };
    return this;
  });

}).call(this);

(function() {
  var HTTPPoller, Poller, module;

  module = angular.module('h2o.services');

  Poller = (function() {
    function Poller(id, pollingInterval) {
      this.id = id;
      this.pollingInterval = pollingInterval;
      this.isPolling = false;
      this.pollingPromise = null;
    }

    return Poller;

  })();

  HTTPPoller = (function() {
    function HTTPPoller(id, pollEndpoint) {
      this.id = id;
      this.pollEndpoint = pollEndpoint;
      return;
    }

    return HTTPPoller;

  })();

  module.service('PollerService', function($http, $log, $rootScope, $interval) {
    var _this = this;
    this.pollers = {};
    this.callbacks = {};
    this.addPoller = function(id, pollingInterval, callback) {
      var poller;
      if (this.pollers[id] != null) {
        return null;
      }
      poller = new Poller(id, pollingInterval);
      this.pollers[poller.id] = poller;
      this.callbacks[poller.id] = callback;
      return poller;
    };
    this.startPolling = function(poller) {
      poller.isPolling = true;
      return _this.reschedule(poller);
    };
    this.stopPolling = function(poller) {
      poller.isPolling = false;
      return _this.reschedule(poller);
    };
    this.reschedule = function(poller) {
      var pollCallback;
      if (poller.isPolling && !poller.pollingPromise) {
        pollCallback = function() {
          return _this.poll(poller);
        };
        poller.pollingPromise = $interval(pollCallback, poller.pollingInterval);
      }
      if (!poller.isPolling && poller.pollingPromise) {
        $interval.cancel(poller.pollingPromise);
        return poller.pollingPromise = null;
      }
    };
    this.poll = function(poller) {
      _this.callbacks[poller.id](poller);
      return _this.reschedule(poller);
    };
  });

  module.service('HTTPPollerService', function($http, $log, $rootScope, $interval, PollerService, ErrorAndStatusService) {
    var _this = this;
    this.pollers = {};
    this.httpPollers = {};
    this.callbacks = {};
    this.addPoller = function(id, pollingInterval, pollEndpoint, callback) {
      var httpPoller, poller,
        _this = this;
      if (this.httpPollers[id] != null) {
        return null;
      }
      httpPoller = new HTTPPoller(id, pollEndpoint);
      this.httpPollers[id] = httpPoller;
      this.callbacks[id] = callback;
      poller = PollerService.addPoller(id, pollingInterval, function(poller) {
        httpPoller = _this.httpPollers[poller.id];
        return _this.poll(httpPoller);
      });
      this.pollers[id] = poller;
      return httpPoller;
    };
    this.startPolling = function(httpPoller) {
      return PollerService.startPolling(_this.pollers[httpPoller.id]);
    };
    this.stopPolling = function(httpPoller) {
      return PollerService.stopPolling(_this.pollers[httpPoller.id]);
    };
    this.cacheBustURL = function(url) {
      var cacheBustQuery, hasQuery;
      cacheBustQuery = "" + (new Date().getTime());
      hasQuery = ~url.indexOf('?');
      if (hasQuery) {
        return "" + url + "&" + cacheBustQuery;
      } else {
        return "" + url + "?" + cacheBustQuery;
      }
    };
    this.poll = function(poller) {
      var url;
      if (ErrorAndStatusService.isLoading(poller.id)) {
        return;
      }
      ErrorAndStatusService.clearErrors({
        sender: poller.id,
        type: "poller"
      });
      ErrorAndStatusService.setStatus(poller.id, "loading");
      url = _this.cacheBustURL((poller.pollEndpoint instanceof Function ? poller.pollEndpoint() : poller.pollEndpoint));
      return $http({
        method: "GET",
        url: url
      }).success(function(data, status_code, headers, config) {
        ErrorAndStatusService.setStatus(poller.id, "ok");
        return _this.callbacks[poller.id](data, ErrorAndStatusService.status(poller.id), status_code);
      }).error(function(data, status_code, headers, config) {
        var message, _ref;
        message = (_ref = (data ? data.error : null)) != null ? _ref : "unknown error";
        ErrorAndStatusService.setStatus(poller.id, "error");
        ErrorAndStatusService.addError(poller.id, {
          type: "poller",
          message: message
        });
        return _this.callbacks[poller.id](data, ErrorAndStatusService.status(poller.id), status_code);
      });
    };
  });

}).call(this);

(function() {
  var ClusterStatistics, Node, module, _ref, _ref1,
    __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
    __hasProp = {}.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

  module = angular.module('h2o.services');

  Node = (function(_super) {
    __extends(Node, _super);

    function Node() {
      this.maxMemPercent = __bind(this.maxMemPercent, this);
      this.usedMemPercent = __bind(this.usedMemPercent, this);
      this.totMemPercent = __bind(this.totMemPercent, this);
      this.usedMemBytes = __bind(this.usedMemBytes, this);
      _ref = Node.__super__.constructor.apply(this, arguments);
      return _ref;
    }

    Node.prototype.usedMemBytes = function() {
      return this.totMemBytes - this.freeMemBytes;
    };

    Node.prototype.totMemPercent = function() {
      if (this.maxMemBytes) {
        return 100.0 * (this.totMemBytes / this.maxMemBytes);
      } else {
        return 0;
      }
    };

    Node.prototype.usedMemPercent = function() {
      if (this.maxMemBytes) {
        return 100.0 * (this.usedMemBytes() / this.maxMemBytes);
      } else {
        return 0;
      }
    };

    Node.prototype.maxMemPercent = function() {
      if (this.maxMemBytes) {
        return 100.0 - this.totMemPercent - this.usedMemPercent;
      } else {
        return 0;
      }
    };

    Node.prototype.update = function(data) {
      this.name = Model.sanitizeString(data.name);
      this.numKeys = Model.sanitizeInt(data.num_keys);
      this.valueSizeBytes = Model.sanitizeInt(data.value_size_bytes);
      this.freeMemBytes = Model.sanitizeInt(data.free_mem_bytes);
      this.totMemBytes = Model.sanitizeInt(data.tot_mem_bytes);
      this.maxMemBytes = Model.sanitizeInt(data.max_mem_bytes);
      this.freeDiskBytes = Model.sanitizeInt(data.free_disk_bytes);
      this.maxDiskBytes = Model.sanitizeInt(data.max_disk_bytes);
      this.numCpus = Model.sanitizeInt(data.num_cpus);
      this.systemLoad = Model.sanitizeFloat(data.system_load);
      this.elapsedTime = Model.sanitizeInt(data.elapsed_time);
      this.nodeHealthy = Model.sanitizeBool(data.node_healthy);
      this.rpcs = Model.sanitizeInt(data.rpcs);
      this.tcpsActive = Model.sanitizeInt(data.tcps_active);
      this.openFds = Model.sanitizeFloat(data.open_fds);
      this.myCpuPercent = Model.sanitizeFloat(data['my_cpu_%']);
      this.sysCpuPercent = Model.sanitizeFloat(data['sys_cpu_%']);
      this.lastContact = Model.sanitizeInt(data.last_contact);
      return Node.__super__.update.call(this, data);
    };

    return Node;

  })(Model);

  ClusterStatistics = (function(_super) {
    __extends(ClusterStatistics, _super);

    function ClusterStatistics() {
      _ref1 = ClusterStatistics.__super__.constructor.apply(this, arguments);
      return _ref1;
    }

    ClusterStatistics.prototype.update = function(data) {
      var defaultTo;
      this.version = Model.sanitizeString(data.version);
      this.cloudName = Model.sanitizeString(data.cloud_name);
      this.nodeName = Model.sanitizeString(data.node_name);
      this.cloudSize = Model.sanitizeInt(data.cloud_size, defaultTo = 1);
      this.cloudHealthy = Model.sanitizeBool(data.cloud_healthy);
      this.consensus = Model.sanitizeBool(data.consensus);
      this.locked = Model.sanitizeBool(data.locked);
      this.nodes = Model.sanitizeArray(data.nodes, Node);
      return ClusterStatistics.__super__.update.call(this, data);
    };

    return ClusterStatistics;

  })(Model);

  module.service('StatisticsPollerDataService', function($http, $log, $rootScope, $interval, HTTPPollerService, ErrorAndStatusService) {
    var _this = this;
    this.clusterStatistics = new ClusterStatistics();
    this.hadFirstContact = function() {
      return ErrorAndStatusService.isInitialized(this.poller.id);
    };
    this.pollEndpoint = "/Cloud.json";
    this.init = function() {
      return this.poller = HTTPPollerService.addPoller("StatisticsPollerDataService", 1000, this.pollEndpoint, this.updateData);
    };
    this.startPolling = function() {
      return HTTPPollerService.startPolling(this.poller);
    };
    this.stopPolling = function() {
      return HTTPPollerService.stopPolling(this.poller);
    };
    this.updateData = function(data, http_status, status_code) {
      return _this.clusterStatistics.update(data);
    };
    this.init();
    return this;
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.services.typeahead');

  module.service('TypeaheadService', function($log) {
    this.keyDataset = {
      name: 'key',
      remote: {
        url: '/TypeaheadKeysRequest?filter=%QUERY&limit=1024',
        filter: function(response) {
          var item, _i, _len, _ref, _results;
          _ref = response.items;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            item = _ref[_i];
            _results.push({
              value: item,
              tokens: [item]
            });
          }
          return _results;
        }
      }
    };
    this.s3Dataset = {
      name: 's3',
      remote: {
        url: '/TypeaheadS3BucketRequest?filter=%QUERY&limit=1024',
        filter: function(response) {
          var item, _i, _len, _ref, _results;
          _ref = response.items;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            item = _ref[_i];
            _results.push({
              value: item,
              tokens: [item]
            });
          }
          return _results;
        }
      }
    };
    this.clusterDataset = {
      name: 'cluster',
      remote: {
        url: '/TypeaheadFileRequest?filter=%QUERY&limit=1024',
        filter: function(response) {
          var item, _i, _len, _ref, _results;
          _ref = response.items;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            item = _ref[_i];
            _results.push({
              value: item,
              tokens: [item]
            });
          }
          return _results;
        }
      }
    };
    return this;
  });

}).call(this);

(function() {
  var module;

  module = angular.module('h2o.services');

  module.service('DialogService', function($http, $log, $rootScope) {
    var _this = this;
    this.dialogs = {};
    this.groupNameFromID = function(id) {
      var components;
      components = id.split(".");
      if (components.length > 1) {
        return components[0];
      } else {
        return null;
      }
    };
    this.belongsToGroup = function(id, group) {
      return _this.groupNameFromID(id) === group;
    };
    this.IDsInGroup = function(group) {
      var id, isOpen, result, _ref;
      result = [];
      _ref = _this.dialogs;
      for (id in _ref) {
        isOpen = _ref[id];
        if (_this.belongsToGroup(id, group)) {
          result.push(id);
        }
      }
      return result;
    };
    this.isOpen = function(id) {
      var _ref;
      return (_ref = _this.dialogs[id]) != null ? _ref : false;
    };
    this.toggleOpen = function(id) {
      if (_this.isOpen(id)) {
        return _this.close(id);
      } else {
        return _this.open(id);
      }
    };
    this.open = function(id) {
      var group, otherInSameGroupID, _i, _len, _ref;
      group = _this.groupNameFromID(id);
      _ref = _this.IDsInGroup(group);
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        otherInSameGroupID = _ref[_i];
        _this.close(otherInSameGroupID);
      }
      _this.dialogs[id] = true;
      return $rootScope.$broadcast("DialogOpened", id);
    };
    this.close = function(id) {
      _this.dialogs[id] = false;
      return $rootScope.$broadcast("DialogClosed", id);
    };
    return this;
  });

}).call(this);

(function() {
  var ErrorMessage, module,
    __indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

  module = angular.module('swing.services.errorAndStatus');

  this.LOADING_STATUS = {
    "start": 0,
    "ok": 1,
    "loading": 2,
    "error": 3
  };

  this.ERROR_LEVELS = ["debug", "info", "warn", "error", "success"];

  this.ErrorAndStatusServiceErrorsChangedEvent = "ErrorAndStatusServiceErrorsChanged";

  this.ErrorAndStatusServiceStatusesChangedEvent = "ErrorAndStatusServiceStatusesChanged";

  ErrorMessage = (function() {
    function ErrorMessage() {
      this.uuid = uuid();
      return;
    }

    ErrorMessage.prototype.update = function(sender, level, type, subject, message) {
      this.sender = sender;
      this.level = level;
      this.type = type;
      this.subject = subject;
      this.message = message;
      this.timestamp = new Date().getTime();
    };

    ErrorMessage.prototype.fitsPattern = function(_arg) {
      var level, levelIn, levelNot, levelNotIn, sender, senderIn, senderNot, senderNotIn, subject, subjectIn, subjectNot, subjectNotIn, type, typeIn, typeNot, typeNotIn, _ref, _ref1, _ref2, _ref3, _ref4, _ref5, _ref6, _ref7;
      sender = _arg.sender, senderNot = _arg.senderNot, level = _arg.level, levelNot = _arg.levelNot, type = _arg.type, typeNot = _arg.typeNot, subject = _arg.subject, subjectNot = _arg.subjectNot;
      if (sender != null) {
        senderIn = sender instanceof Array ? sender : [sender];
      }
      if (level != null) {
        levelIn = level instanceof Array ? level : [level];
      }
      if (type != null) {
        typeIn = type instanceof Array ? type : [type];
      }
      if (subject != null) {
        subjectIn = subject instanceof Array ? subject : [subject];
      }
      if (senderNot != null) {
        senderNotIn = senderNot instanceof Array ? senderNot : [senderNot];
      }
      if (levelNot != null) {
        levelNotIn = levelNot instanceof Array ? levelNot : [levelNot];
      }
      if (typeNot != null) {
        typeNotIn = typeNot instanceof Array ? typeNot : [typeNot];
      }
      if (subjectNot != null) {
        subjectNotIn = subjectNot instanceof Array ? subjectNot : [subjectNot];
      }
      if (((senderIn != null) && (_ref = this.sender, __indexOf.call(senderIn, _ref) < 0)) || ((senderNotIn != null) && (_ref1 = this.sender, __indexOf.call(senderNotIn, _ref1) >= 0))) {
        return false;
      }
      if (((levelIn != null) && (_ref2 = this.level, __indexOf.call(levelIn, _ref2) < 0)) || ((levelNotIn != null) && (_ref3 = this.level, __indexOf.call(levelNotIn, _ref3) >= 0))) {
        return false;
      }
      if (((typeIn != null) && (_ref4 = this.type, __indexOf.call(typeIn, _ref4) < 0)) || ((typeNotIn != null) && (_ref5 = this.type, __indexOf.call(typeNotIn, _ref5) >= 0))) {
        return false;
      }
      if (((subjectIn != null) && (_ref6 = this.subject, __indexOf.call(subjectIn, _ref6) < 0)) || ((subjectNotIn != null) && (_ref7 = this.subject, __indexOf.call(subjectNotIn, _ref7) >= 0))) {
        return false;
      }
      return true;
    };

    return ErrorMessage;

  })();

  window.ErrorMessage = ErrorMessage;

  module.service('ErrorAndStatusService', function($http, $log, $rootScope) {
    var _this = this;
    this.statuses = {};
    this.errors = [];
    this.dirtyUUIDs = [];
    this.init = function() {
      var _this = this;
      return $rootScope.$watch((function() {
        return _this.dirtyUUIDs;
      }), this.commitPendingErrorBroadcast, true);
    };
    this.markErrorAsDirty = function(error) {
      return this.dirtyUUIDs.push(error.uuid);
    };
    this.commitPendingErrorBroadcast = function() {
      if (_this.dirtyUUIDs.length) {
        $rootScope.$broadcast(ErrorAndStatusServiceErrorsChangedEvent);
      }
      return _this.dirtyUUIDs = [];
    };
    this.setStatus = function(sender, status) {
      if (LOADING_STATUS[status] == null) {
        $log.error("" + sender + " tried to set an inexistant status of " + status + ".");
        return;
      }
      this.statuses[sender] = LOADING_STATUS[status];
      return this.statusChangedBroadcast();
    };
    this.statusChangedBroadcast = function() {
      return $rootScope.$broadcast(ErrorAndStatusServiceStatusesChangedEvent);
    };
    this.status = function(sender) {
      return this.statuses[sender];
    };
    this.isRegistered = function(sender) {
      return this.statuses[sender] != null;
    };
    this.isUninitialized = function(sender) {
      return !this.isRegistered(sender) || this.status(sender) === LOADING_STATUS.start;
    };
    this.isInitialized = function(sender) {
      return this.isRegistered(sender) && this.status(sender) > LOADING_STATUS.start;
    };
    this.isLoading = function(sender) {
      return this.isRegistered(sender) && this.status(sender) === LOADING_STATUS.loading;
    };
    this.isOK = function(sender) {
      return this.isRegistered(sender) && this.status(sender) === LOADING_STATUS.ok;
    };
    this.isDistressed = function(sender) {
      return this.isRegistered(sender) && this.status(sender) === LOADING_STATUS.error;
    };
    this.addError = function(sender, _arg) {
      var error, level, message, oldErrors, replace, subject, type;
      level = _arg.level, type = _arg.type, subject = _arg.subject, message = _arg.message, replace = _arg.replace;
      if (replace == null) {
        replace = true;
      }
      if (sender == null) {
        $log.error("Tried to add an error with no 'sender' set.");
        return;
      }
      if (message == null) {
        $log.error("" + sender + " tried to add an error with no 'message' set.");
        return;
      }
      if ((level != null) && __indexOf.call(typeof ERROR_LEVELS !== "undefined" && ERROR_LEVELS !== null, level) < 0) {
        $log.error("" + sender + " tried to set an inexistant error level of " + level + ".");
        return;
      }
      error = null;
      if (replace) {
        oldErrors = this.errorsThatFitPattern({
          sender: sender,
          level: level,
          type: type,
          subject: subject
        });
        if (oldErrors.length > 0) {
          error = oldErrors[0];
        }
        if (oldErrors.length > 1) {
          oldErrors.splice(0, 1);
          this.removeErrors(oldErrors);
        }
      }
      if (error == null) {
        error = new ErrorMessage();
        error.update(sender, level, type, subject, message);
        this.errors.push(error);
        $log.debug("[NEW ERROR]: " + JSON.stringify(error));
      } else {
        error.update(sender, level, type, subject, message);
        $log.debug("[UPDATED ERROR]: " + JSON.stringify(error));
      }
      return this.markErrorAsDirty(error);
    };
    this.errorsThatFitPattern = function(_arg) {
      var level, levelNot, sender, senderNot, subject, subjectNot, type, typeNot;
      sender = _arg.sender, senderNot = _arg.senderNot, level = _arg.level, levelNot = _arg.levelNot, type = _arg.type, typeNot = _arg.typeNot, subject = _arg.subject, subjectNot = _arg.subjectNot;
      return this.errors.filter(function(error) {
        return error.fitsPattern({
          sender: sender,
          senderNot: senderNot,
          level: level,
          levelNot: levelNot,
          type: type,
          typeNot: typeNot,
          subject: subject,
          subjectNot: subjectNot
        });
      });
    };
    this.errorsThatDontFitPattern = function(_arg) {
      var level, levelNot, sender, senderNot, subject, subjectNot, type, typeNot;
      sender = _arg.sender, senderNot = _arg.senderNot, level = _arg.level, levelNot = _arg.levelNot, type = _arg.type, typeNot = _arg.typeNot, subject = _arg.subject, subjectNot = _arg.subjectNot;
      return this.errors.filter(function(error) {
        return !error.fitsPattern({
          sender: sender,
          senderNot: senderNot,
          level: level,
          levelNot: levelNot,
          type: type,
          typeNot: typeNot,
          subject: subject,
          subjectNot: subjectNot
        });
      });
    };
    this.clearErrors = function(_arg) {
      var level, levelNot, removedErrors, sender, senderNot, subject, subjectNot, type, typeNot;
      sender = _arg.sender, senderNot = _arg.senderNot, level = _arg.level, levelNot = _arg.levelNot, type = _arg.type, typeNot = _arg.typeNot, subject = _arg.subject, subjectNot = _arg.subjectNot;
      removedErrors = this.errorsThatFitPattern({
        sender: sender,
        senderNot: senderNot,
        level: level,
        levelNot: levelNot,
        type: type,
        typeNot: typeNot,
        subject: subject,
        subjectNot: subjectNot
      });
      return this.removeErrors(removedErrors);
    };
    this.removeErrors = function(errors) {
      var error;
      if (errors == null) {
        errors = [];
      }
      return this.removeErrorsByUUIDs((function() {
        var _i, _len, _results;
        _results = [];
        for (_i = 0, _len = errors.length; _i < _len; _i++) {
          error = errors[_i];
          _results.push(error.uuid);
        }
        return _results;
      })());
    };
    this.removeErrorsByUUIDs = function(errorUUIDs) {
      var removedError, removedErrors, _i, _len;
      if (errorUUIDs == null) {
        errorUUIDs = [];
      }
      removedErrors = this.errors.filter(function(error) {
        var _ref;
        return _ref = error.uuid, __indexOf.call(errorUUIDs, _ref) >= 0;
      });
      this.errors = this.errors.filter(function(error) {
        var _ref;
        return _ref = error.uuid, __indexOf.call(errorUUIDs, _ref) < 0;
      });
      for (_i = 0, _len = removedErrors.length; _i < _len; _i++) {
        removedError = removedErrors[_i];
        $log.debug("[ERROR REMOVED]: " + JSON.stringify(removedError));
        this.markErrorAsDirty(removedError);
      }
      return removedErrors.length;
    };
    this.init();
    return this;
  });

}).call(this);
