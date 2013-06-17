REM Set up variables and environment variables.
set dirname=%0\..
chdir %dirname%\..
set H2O_DIR=%CD%
set jre_version=jre1.7.0_21-windows-x64
set JAVA_HOME=%H2O_DIR%\windows\%jre_version%\java-windows
set jar_file=H2OLauncher.jar

REM Run H2OLauncher in the background.
start "H2OLauncher" /B "%JAVA_HOME%\bin\java.exe" -ea -Xmx128m -jar "%jar_file%"
