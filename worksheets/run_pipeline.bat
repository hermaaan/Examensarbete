@echo off
REM Aktivera din Python-virtualenv (om du använder det)
call C:\Users\Herman\Documents\STI\Examensarbete2024\Examensarbete\.venv\Scripts\activate

REM Kör Python-skriptet för att ladda data
python C:\Users\Herman\Documents\STI\Examensarbete2024\Examensarbete\Worksheets\load_data.py

REM Navigera till DBT-projektet
cd /d C:\Users\Herman\Documents\STI\Examensarbete2024\Examensarbete\DBT\dbt_code_bq

REM Kör DBT
dbt run

REM Logga körningen
echo Pipeline körd: %date% %time% >> C:\Users\Herman\Documents\STI\Examensarbete2024\Examensarbete\Worksheets\logfile.log