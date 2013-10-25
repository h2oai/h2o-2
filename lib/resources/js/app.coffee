app = angular.module('h2o.app', [])

app.service 'H2OMenuService', ($http, $log, $rootScope) ->
	@menus = {}

	@isOpen = (menu) =>
		@menus[menu] ? false

	@toggleOpen = (menu) =>
		@menus[menu] = not @isOpen menu
