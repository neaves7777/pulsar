@echo off

cd /d %~dp0

rem ojdbc6.jar

rem sqljdbc4.jar

rem jt400.jar

java.exe -classpath ionData.jar;sqljdbc4.jar ionData.ionData %1 %2
