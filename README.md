# national-register-scripts
Scripts for extracting data from the US National Register of Historic Places spreadsheets and website. Written
and tested against Python 3.6.

The National Park Service provides links to their public data at https://www.nps.gov/subjects/nationalregister/data-downloads.htm. They provide both a GDB database of the National Register of Historic Places, and spreadsheets. As of
this writing, spreadsheets are updated through April 2019. 

Weekly updates are provided at https://www.nps.gov/subjects/nationalregister/weekly-list.htm.

The spreadsheet handling code here assumes the .xlsx files are in your current directory.

Sample invocatons:
* `python36 ShowPreviews.py` shows a few lines from each of the spreadsheets.
* Choose your own skip and sample size, for example `sp.preview('removed_20190404.xlsx', skip=200, count=30)`.
* `python36 FetchAndProcessLatestWeeklyNPSUpdate.py` grabs the weekly updates and parses them.

Required Python libraries:
* Beautiful Soup (HTML parser)
* xlrd (Excel library)
* psycopg2 (PostgreSQL interface)
* json, urllib, re, time, datetime

