@echo off
call C:\ProgramData\Anaconda3\Scripts\activate.bat C:\ProgramData\Anaconda3
call conda env remove --name ncdes
call conda env create --name ncdes --file environment.yml
call conda activate ncdes
python -m ncdes.main
pause