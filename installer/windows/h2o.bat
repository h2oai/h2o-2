# Set up variables and environment variables.
for %%F in ("%0") do set dirname=%%~dpF
echo %dirname%
set jre_version=jre1.7.0_21-windows-x64
set JAVA_HOME=%dirname%windows\%jre_version%\java-windows
set H2O_DIR=%dirname%
set jar_file=H2OLauncher.jar

# Run H2OLauncher in the background. 
chdir $H2O_DIR
start "H2OLauncher" /B "%JAVA_HOME%\bin\java.exe" -ea -Xmx128m -jar %jar_file%
