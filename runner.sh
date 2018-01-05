#!/bin/bash

importData="false"
if [ $# -eq 1 ]
  then
  	if [ "$1" == "--import" ]
      then
  	    importData="true"
  	fi
fi

#Import data if specified
if [ "$importData" == "true" ]; then
  STARTTIME=$(date +%s)

  echo "Clearing database in case it already exists"
  mongo hway_proj_db --eval "db.dropDatabase()"

  echo "Importing highways"
  mongoimport -d hway_proj_db -c highways --type csv --file ProjectData-2017/highways.csv --headerline
  mongoimport -d hway_proj_db -c readings --type csv --file ProjectData-2017/freeway_loopdata.csv --headerline
  mongoimport -d hway_proj_db -c detectors --type csv --file ProjectData-2017/freeway_detectors.csv --headerline
  mongoimport -d hway_proj_db -c stations --type csv --file ProjectData-2017/freeway_stations.csv --headerline

  ENDTIME=$(date +%s)
  echo "Import took $(($ENDTIME - $STARTTIME)) seconds to to complete..."
fi

#Run python script to answer queries
python db_proj.py
