$(function () {
    // For each <span class='label mref'>X > Y</span> register 
    // a event listener which causes showing menu X and high-lighting
    // its item Y.

    // This is a hack since we do not generate id for menu items.
    // However, we need them only in tutorial page, so it is better
    // to generate them in JS.

    // Get all menu items top-level menu and generate them id
    $('ul.nav > li').children('.dropdown-toggle').each(function() {
        var uid = 'uid'+$(this).text().replace(/ /g,'_');
        $(this).attr('id', uid);
        $(this).parent().children('ul').children('li').children('a').each(function() {
            var iuid = uid + $(this).text().replace(/ /g,'_');
            $(this).attr('id', iuid);
        });
    });

    // Get all span referencing the menu and register
    // correspoding listeners.
    $('span.label.mref').each(addMenuToggle);
    
    function addMenuToggle(index) {
        var path =  $(this).text().split(' > ');
        var menuId = '#uid'+path[0].replace(/ /g, '_');
        var itemId = menuId+path[1].replace(/ /g, '_');
        var menu = $(menuId);
        var item = $(menuId).parent().children('ul').children('li').children(itemId);
        $(this).bind('mouseenter', function() { 
            menu.dropdown('toggle');
            item.toggleClass('highlight');
        });
    }
});

