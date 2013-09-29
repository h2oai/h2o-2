
echo "Removing any existing .Renviron and .Rprofile and recreating"
rm -f ~/.Renviron
rm -f ~/.Rprofile
# Set CRAN mirror to a default location
echo "options(repos = \"http://cran.stat.ucla.edu\")" > ~/.Rprofile
# Needed so the h2oWrapper doesn't try installing to a site library
# normally first in .libPaths() in R, and only writeable by root.
# .libPaths() will show this first now
echo "R_LIBS_USER=\"~/.Rlibrary\"" > ~/.Renviron
# I don't rm it if it already exits
mkdir -p ~/.Rlibrary
