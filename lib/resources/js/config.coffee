@JSONApiServerURI = () ->
	baseURI = URI window.location
	
	baseURI.path "/Inspect"

	baseURI.clone()
