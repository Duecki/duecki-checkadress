#!/usr/bin/python
# -*- encoding: utf-8 -*-
# https://pypi.python.org/pypi/geocoder/
from pymongo import MongoClient
import geocoder
import json
import time
from pprint import pprint
import googlemaps
import MySQLdb
import sys

def mongoconnect():
    start = time.time()
    mongoconnect = MongoClient(mongoDB_SERVER, mongoDB_PORT)
    mdb = mongoconnect.TeslaLog
    mdb.authenticate(mongoDB_USER,mongoDB_PWD)
    posts = mdb.logbook_clone
    print "Mongoconnect:",time.time() - start
    return(posts)




def mongocheck():
    global googlerequestcount
    global mongoupdates
    drive = False
    trippnumber = 0
    print "mongocheck.start"
    try:
        posts = mongoconnect()
    except:
        print "Fehler im DB connect"
#        sys.exit(1)

    rawdata = posts.find({"shift_state":{"$exists":True}},{"_id":1,"messZeit":1,"KMstand":1,"shift_state":1}).sort("_id",1).limit(mongoupdatelimit)

    i = 0
    for dd in rawdata:
        if i == 0:
            i = 1
            print "set first endKM"
            endKM = float(dd['KMstand'])

        if drive and dd['shift_state'] == "D":
            posts.update_one({'_id': dd['_id']}, {"$set": { "due_trippnumber": trippnumber, "due_trippstatus": "run"}})
            mongoupdates += 1
        elif dd['shift_state'] == "D":
#        elif float(dd['KMstand']) > endKM and not drive:
            ## START
            trippnumber += 1
            drive = True
            startKM = float(prevdd['KMstand'])
            startZeit = dd['messZeit']
            startID = prevdd['_id']
            posts.update_one({'_id': prevdd['_id']}, {"$set": { "due_trippnumber": trippnumber, "due_trippstatus": "start"}})
            mongoupdates += 1
            posts.update_one({'_id': dd['_id']}, {"$set": { "due_trippnumber": trippnumber, "due_trippstatus": "run"}})
            mongoupdates += 1

            if trippnumber != 1:
#                print "-- start:",startKM, "  endKM:",endKM
                tdelta = float(startKM) - float(endKM)
                if tdelta > 5:
                    print " "
                    print "OBACHT! Delta:", startKM - endKM, "Zeit:", dd['messZeit'], "-------------------------------"
                    print " "


            print trippnumber, " Startzeit: ",dd['messZeit'], "StartKM:   ",prevdd['KMstand'],
            #print "ID:        ",dd['_id']
        elif drive and dd['shift_state'] != "D":
            ## END
            drive = False
            distanz = float(dd['KMstand']) - startKM
            posts.update_one({'_id': dd['_id']}, {"$set": { "due_trippnumber": trippnumber, "due_trippstatus": "end", "due_trippdistanz", distanz}})
            mongoupdates += 1
            endKM = float(dd['KMstand'])
            print "EndKM:     ",dd['KMstand'], "Distanz:   ",distanz,
            try:
                dauer = dd['messZeit'] - startZeit
                print "Dauer: ",dauer
            except:
                print " "
        else:
            drive = False
        prevdd = dd




def mysqlcheck():
    global mysqlupdates
    global mysqljobs
    if debugmode:
        print "\nmysqlcheck.start"
    try:
        db = MySQLdb.connect(connect_timeout=10,host=DB_SERVER,user=DB_USER,passwd=DB_PWD,db=DB_DATENBANK)
    except:
        print "Probleme beim Aufbau zur mysql verbindung"
    cursor = db.cursor()
    if debugmode:
        print "\nmysqlconnect erfolg",
    SQLQUERY = ("""SELECT COUNT(eintrag) FROM `TeslaLogDB`.`teslatracking` WHERE city is NULL;""")
    cursor.execute(SQLQUERY)
    tmp = cursor.fetchone()
    mysqljobs = tmp[0]


#    format_str = """INSERT INTO employee (staff_number, fname, lname, gender, birth_date)
#    VALUES (NULL, "{first}", "{last}", "{gender}", "{birthdate}");"""

#    sql_command = format_str.format(first=p[0], last=p[1], gender=p[2], birthdate = p[3])
#    cursor.execute(sql_command)


    format_str = """SELECT eintrag, longitude, latitude, date, city FROM TeslaLogDB.teslatracking where city is  NULL AND longitude <> 0 AND latitude <> 0 LIMIT {sqllimit};"""
    SQLQUERY = format_str.format(sqllimit=mysqllimit)
    cursor.execute(SQLQUERY)

    posts = mongoconnect()
#    cursor.execute("""SELECT eintrag, longitude, latitude, date, city FROM TeslaLogDB.teslatracking where city is  NULL AND longitude <> 0 AND latitude <> 0 LIMIT 2000""")
    for (eintrag, longitude, latitude, date, city) in cursor :
        geoatri={
            'longi':longitude,
            'lati':latitude,
            'glogin':glogin,
            'posts':posts
        }
        geoinfo = dgeoinfo(geoatri)

        if debugmode:
            print "mysqlcheck.dgeofinforeturn:"
            pprint (geoinfo)
        eint = int(eintrag)
        if mysqlupdate:
            cursor.execute("""UPDATE TeslaLogDB.teslatracking SET city = %s, street = %s, housenumber = %s WHERE eintrag = %s""",[geoinfo['City'],geoinfo['Street'],geoinfo['Housenumber'],eint])
            db.commit()
            mysqlupdates += 1
            if debugmode:
                print "mysqlcheck.wmysql"
    db.close()



########################################################################
########################################################################
########################################################################
###start Hauptprogram


debugmode = False
swmongocheck = False
swmysqlcheck = False
mongoupdate = True
mysqlupdate = True
askgoogle = False
data = []
mysqllimit = 10
mongoupdatelimit = 10
googlerequestcount = 0
mysqlupdates = 0
mongoupdates = 0
mysqljobs = 0
mongojobs = 0



#print "länge",len(sys.argv)
#ufruf = sys.argv

if len(sys.argv) > 1:
    print "Programmstart"
else:
    print "Bitte Parameter mit geben:"
    print "debugmode"
    print "mongocheck"
    print "mysqlcheck"
    print "mysqllimit <Wert>"
    print "mongoupdatelimit <Wert>"
    print "\n\n"

i = 0
for startpara in sys.argv:
    i += 1
    if startpara == 'debugmode':
        debugmode = True
    if startpara == "mongocheck":
        swmongocheck = True
    if startpara == "mysqlcheck":
        swmysqlcheck = True
    if startpara == "mysqllimit":
        mysqllimit = sys.argv[i]
    if startpara == "mongoupdatelimit":
        mongoupdatelimit = int(sys.argv[i])



print "SQLLimit:",mysqllimit

#Einlesen der Configuration
if debugmode:
    print "einlesen der Config"
#	try:
with open ('/home/ubuntu/tesla/getTeslaconfig.json', 'r') as f:
    getTeslaconf = json.load(f)

DB_SERVER = getTeslaconf['DB_SERVER']
DB_USER = getTeslaconf['DB_USER']
DB_PWD = getTeslaconf['DB_PWD']
DB_DATENBANK = getTeslaconf['DB_DATENBANK']
mongoDB_SERVER = getTeslaconf['mongoDB_SERVER']
mongoDB_DATENBANK = getTeslaconf['mongoDB_DATENBANK']
mongoDB_USER = getTeslaconf['mongoDB_USER']
mongoDB_PWD = getTeslaconf['mongoDB_PWD']
mongoDB_PORT = int(getTeslaconf['mongoDB_PORT'])
# nicht implementiert: mongoDB_COLLECTION = getTeslaconf['mongoDB_COLLECTION']
glogin = getTeslaconf['glogin']


startzeit = time.time()
if swmongocheck:
    print "\n\nmongocheck"
    mongocheck()

if swmysqlcheck:
    print "\n\nmysqlcheck"
    print "mysqlcheck noch nicht vorhanden"
#    mysqlcheck()

print ""
print "Googleanfragen:", googlerequestcount
print "mysqljobs:",mysqljobs
print "monogupdates:",mongoupdates
print "mysqlupdates",mysqlupdates
print "Laufzeit: ", time.time() - startzeit

print "-----ENDE-------------"
#pprint (ausgabe)
