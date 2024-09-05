

@echo off


:: Defining the logfile

set LOGFILE=pipeline_log.txt

:: logging the start time

echo Running pipeline at %date% %time% >> %LOGFILE%

::Running the  and logging the output

C:\Program Files\Python312\python.exe ETL.py >>%LOGFILE% 2>&1


:: logging the end time

echo Pipeline completed at %date% %time% >> %LOGFILE%

echo. >> %LOGFILE%