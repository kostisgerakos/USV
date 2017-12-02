@echo off
start /wait "" App/MissionPlanner-latest.msi
start /wait "" App/IronPython-2.7.7.msi
start /wait "" App/x64/DB.Browser.for.SQLite-3.9.1-win64.exe
start /wait "" App/x64/python-2.7.13.amd64.msi
IF EXIST ..\Database\MP.db DEL /F ..\Database\MP.db
xcopy ..\Database\Initial\MP.db ..\Database\.
start /wait "" "C:\Program Files (x86)\DB Browser for SQLite\DB Browser for SQLite.exe" "../Database\MP.db"
C:\Python27\python.exe -m pip install -r App/requirements.txt
pause