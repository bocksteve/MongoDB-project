from pymongo import MongoClient
import time

client = MongoClient()

mongo = client.hway_proj_db

print "Detectors count: " + str(mongo.detectors.find().count())
print "Highways count: " + str(mongo.highways.find().count())
print "Stations count: " + str(mongo.stations.find().count())
print "Readings count: " + str(mongo.readings.find().count()) + "\n"

def query_one():
    print "Query 1(a): How many speeds greater than 90?"

    start_time = time.time()

    query_one_result = mongo.readings.find({'speed' : {'$gt' : 90}}).count()

    elapsed_time = time.time() - start_time

    print "\tResult=" + str(query_one_result)
    print "\tComputed in:" + str(elapsed_time) + " seconds"+ "\n"

def query_two():
    print "Query 2(b): What is the total volume for Divison SB on October 4, 2011?"

    start_time = time.time()

    division_sb = mongo.stations.find_one({'locationtext' : 'Division SB'})

    detectors = mongo.detectors.find({'stationid' : division_sb['stationid']})
    detector_ids = []
    for detector in detectors:
        detector_ids.append(detector['detectorid'])

    readings = mongo.readings.find({'detectorid' : { '$in' : detector_ids}, 'starttime' : { '$regex' : '2011-10-04.*'}})
    totalVolume = 0
    for reading in readings:
        volume = reading['volume']
        if not volume == u'':
            totalVolume += int(volume)

    elapsed_time = time.time() - start_time

    print "\tResult=" + str(totalVolume)
    print "\tComputed in:" + str(elapsed_time) + " seconds"+ "\n"

def query_three():
    print "Query 3(f): Find a route from Sunnyside to Columbia Blvd on I-205 NB using the upstream and downstream fields"

    start_time = time.time()

    sunnyside = mongo.stations.find_one({'locationtext' : 'Sunnyside NB'})
    columbia = mongo.stations.find_one({'locationtext' : 'Columbia to I-205 NB'})

    path = "\tBegin at "+ sunnyside['locationtext'] + "\n"
    dest = mongo.stations.find_one({'upstream' : sunnyside['stationid']})
    while dest['stationid'] != columbia['stationid']:
        path += "\tGo to " + dest['locationtext'] + "\n"
        dest = mongo.stations.find_one({'upstream' : dest['stationid']})
    path += "\tEnd at " + dest['locationtext']

    elapsed_time = time.time() - start_time

    print path
    print "\tComputed in:" + str(elapsed_time) + " seconds" + "\n"

def query_four():
    print "Query 4(d): Peak Period Travel Times: Find the average travel time for 8-9AM and 5-6PM on Oct 04, 2011 for station Foster SB. Report travel time in seconds"

    start_time = time.time()

    foster = mongo.stations.find_one({'locationtext' : 'Foster SB'})

    detectors = mongo.detectors.find({'stationid' : foster['stationid']})
    detector_ids = []
    for detector in detectors:
        detector_ids.append(detector['detectorid'])
    readings = mongo.readings.find({'detectorid' : { '$in' : detector_ids}, 'starttime' : { '$regex' : '2011-10-04 (08|17).*'}})
    speedCount = 0
    speedTotal = 0
    for reading in readings:
        speed = reading['speed']
        if not speed == u'':
            speedCount += int(speed)
            speedTotal += 1

    distance = float(foster['length'])

    elapsed_time = time.time() - start_time

    print "\tTravel time in seconds:" + str(distance / (speedCount/speedTotal) * 3600)
    print "\tComputed in:" + str(elapsed_time) + " seconds\n"

def query_five():
    print "Query 5(e): Find the average travel time for the I-205 NB freeway for 8-9AM and 5-6PM on October 5, 2011"


    start_time = time.time()
    stations = []

    stations = mongo.stations.find({'locationtext' : { '$regex' : '.*NB'}})



    station_travelTimes = 0

    for station in stations:
        station_name = str(station['locationtext'])
        detectors = mongo.detectors.find({'stationid' : station['stationid']})
        detector_ids = []
        for detector in detectors:
            detector_ids.append(detector['detectorid'])

        station_length = station['length']

        readings = []
        readings = mongo.readings.find({'detectorid' : { '$in' : detector_ids}, 'starttime' : { '$regex' : '2011-10-05 (08|17).*'}})

        speeds = []

        for reading in readings:
            speed = reading['speed']
            if not speed == u'':
                speeds.append(speed)

        totalSpeed = 0
        avg_speed = 0
        travel_time = 0
        for speed in speeds:
            totalSpeed += speed

        if len(speeds) > 0:
            avg_speed = totalSpeed/len(speeds)
            travel_time = station_length/avg_speed
            station_travelTimes = station_travelTimes + travel_time

    station_travelTimes = station_travelTimes * 60

    elapsed_time = time.time() - start_time
    print "\tAverage travel time for I-205NB for 8-9PM and 5-6PM = " + str(station_travelTimes) + " minutes"
    print "\tComputed in:" + str(elapsed_time) + " seconds\n"


total_time = time.time()
query_one()
query_two()
query_three()
query_four()
query_five()
elapsed_time = time.time() - total_time
print "Total query execution time:" + str(elapsed_time) + " seconds"
