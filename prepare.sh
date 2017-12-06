#!/usr/bin/env bash

wget https://data.cityofnewyork.us/api/views/wpe2-h2i5/rows.csv?accessType=DOWNLOAD

mv rows.csv?accessType=DOWNLOAD 2010.csv

wget https://data.cityofnewyork.us/api/views/9s88-aed8/rows.csv?accessType=DOWNLOAD

mv rows.csv?accessType=DOWNLOAD 2009.csv

hfs -put 2010.csv
hfs -put 2009.csv
hfs -put degree_original.csv
hfs -put income_original.csv
hfs -put weather.csv
