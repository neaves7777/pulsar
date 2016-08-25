@echo off
rem ServerAlive.cmd
rem Purpose: ping servers and print results to determine 
rem          if server is online and accessible on the network
rem          This is intended to be called from a splunk data input
rem Parameters: List of servernames separated by space
rem Example: ServerAlive jdeprod jdedeploy jdejas01
rem Output Example: 
rem 2014-06-30 20:47:16,host=neaves,message=Reply from 192.168.1.107: bytes=32 time<1ms TTL=128
rem 2014-06-30 20:47:16,host=192.167.1.200,message=Request timed out.
rem 2014-06-30 20:47:16,host=192.168.1.200,message=Reply from 192.168.1.114: Destination host unreachable.
rem 2014-06-30 20:47:16,host=neaves-laptop,message=Reply from fe80::d4d7:7933:2ba8:e7c5%10: time<1ms 

cd /d %~dp0
setlocal enabledelayedexpansion
for /f "skip=1" %%x in ('wmic os get localdatetime') do if not defined MyDate set MyDate=%%x
set today=%MyDate:~0,4%-%MyDate:~4,2%-%MyDate:~6,2% %MyDate:~8,2%:%MyDate:~10,2%:%MyDate:~12,2%



for %%i in (%*) do (
               ping -n 1 %%i > __x
               findstr "Reply Request" __x > __y
               findstr "Average" __x > __z
               set /p response= < __y
               set /p response1= < __z
               echo %today%,server=%%i,message=!response! !response1!
)
del __x __y __z

