:: A Warning note on batch scripted files. Although great for automation, they should be used under caution. 
:: The commands in the batch file are run in unison and do not seek "permissions". If these commands where to be run individually on the computers cmd,
:: then it would offer out warnings should it question the actions you are about to run, giving you a chance to cancel. The .bat file however, does not.

@echo off
setlocal enabledelayedexpansion
echo -----------------------------------------------------------------
echo VENV Installation Script - Helps you create a virtual environment
echo -----------------------------------------------------------------
:: Temporarily disable delayed expansion to check for "!" in the path
setlocal disabledelayedexpansion
echo You are about to create a virtual environment in: %CD%
set "CURRENT_PATH=%CD%"
set "MODIFIED_PATH=%CURRENT_PATH:!=%"
if not "%CURRENT_PATH%"=="%MODIFIED_PATH%" (
    echo WARNING: The current directory contains a "!" character, which may cause issues. Running 'pip install -r requirements' may have trouble installing. Proceed at your own risk.
)
endlocal

setlocal enabledelayedexpansion


echo.
echo --------------
echo Python Version and Virtual Enviroment Name Set Up
echo --------------
:: Update here the Python version and virtual enviroment name should they be different
set SELECTED_PYTHON_VER=3.10
set VENV_NAME=.venv/NCD_Publication
echo Using Python version %SELECTED_PYTHON_VER% to create a virtual enviroment named %VENV_NAME% ...
echo.
py -%SELECTED_PYTHON_VER% -m venv %VENV_NAME% 



echo.
echo ---------------------
echo Upgrading pip install
echo ---------------------
echo Upgrading pip and activating the virtual environment...
call "%VENV_NAME%\Scripts\activate"
"%VENV_NAME%\Scripts\python.exe" -m pip install --upgrade pip  
echo Pip has been upgraded in the virtual environment %VENV_NAME%.




:: Check if requirements.txt exists and handle installation
echo.
echo ---------------------------------------------
echo Installing dependencies from requirements.txt
echo ---------------------------------------------
:: Prompt the user for installation of requirements.txt
if exist requirements.txt (
    echo requirements.txt file has been found.
    pip install -r requirements.txt) else echo requirements.txt not found. Skipping requirements installation. Please execute this manually if still required.

:: List installed packages
echo.
echo Listing installed packages...
pip list
echo.
echo ---------------------------------------------
echo Setup complete. Your virtual environment is ready.
echo To deactivate the virtual environment, type 'deactivate'.
echo ---------------------------------------------




:: Ask user whether they want to run process 1

echo ---------------------------------------------
echo Main Script: Transforming data for publication
echo ---------------------------------------------
set /p RUN_SCRIPT1_NOW="Do you want to run Main Script now? (Please type Y/N and press enter; default = N to avoid accidental running):"
if not defined RUN_SCRIPT1_NOW set RUN_SCRIPT1_NOW="N"
if /I "%RUN_SCRIPT1_NOW%"=="Y" (
    echo User has selected to run Main Script
    python -m ncdes.main) else (
        echo User has NOT selected to run the Main Script. Please refer to guidance for next steps.
    )

echo ------------------------------------------
echo Please check the logging file - if you are happy with the run, then you may now close this command prompt.

cmd /k
endlocal















