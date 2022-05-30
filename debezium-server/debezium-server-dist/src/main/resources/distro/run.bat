
SET PATH_SEP=;
SET JAVA_BINARY=%JAVA_HOME%\bin\java

for %%i in (debezium-server-*runner.jar) do set RUNNER=%%~i
echo %RUNNER%
SET LIB_PATH=lib\*
IF %ENABLE_DEBEZIUM_SCRIPTING%=="true" LIB_PATH=%LIB_PATH%%PATH_SEP%lib_opt\*
call "%JAVA_BINARY%" %DEBEZIUM_OPTS% %JAVA_OPTS% -cp %RUNNER%%PATH_SEP%conf%PATH_SEP%%LIB_PATH% io.debezium.server.Main
