#
# H2OLauncher.jar was created using IDEA's GUI builder.
# The IDEA files all live in ideafiles.zip and the produced jar
# file is checked in so nobody else needs to have IDEA.
#
# The next step here would be to have real windows and mac
# binary apps.
#

default: build

build:
	mkdir -p ../target/launcher
	cp out/artifacts/H2OLauncher_jar/H2OLauncher.jar ../target/launcher/H2OLauncher.jar

makeideazip:
	rm -f ideafiles.zip
	zip -r ideafiles.zip H2OLauncher.iml .idea

makeideafiles:
	unzip ideafiles.zip

clean:

