
SET PATH_SEP=;
SET JAVA_BINARY=%JAVA_HOME%\bin\java

for %%i in (debezium-server-*runner.jar) do set RUNNER=%%~i
echo %RUNNER%
call "%JAVA_BINARY%" %DEBEZIUM_OPTS% %JAVA_OPTS% -cp %RUNNER%%PATH_SEP%conf%PATH_SEP%lib\* io.debezium.server.Main