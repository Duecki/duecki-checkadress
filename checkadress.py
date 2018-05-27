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
    posts = mdb.posts
    print "Mongoconnect:",time.time() - start
    return(posts)


#def dgeoinfo(longi, lati, glogin ,posts):
def dgeoinfo(args):
    global googlerequestcount
    if debugmode:
        print "dgeoinfo:"
        print "dgeoinfo.inputinfos: ",args['longi'],type(longi), args['lati'], type(args['lati']), args['glogin'], type(args['glogin'])
    longi = float(args['longi'])
    lati = float(args['lati'])
    if debugmode:
        print "dgeoinfo.Infos.afterfloatchange: ",args['longi'],type(args['longi']), args['lati'], type(args['lati']), glogin, type(args['glogin'])


#    posts = mongoconnect()
#    if debugmode:
#        print "########","latitude",lati,type(lati),"longitude",longi,type(longi)
#    ausgabe = posts.find_one({"latitude":lati,"longitude":longi,"Housenumber": {"$exists": True}},{"Housenumber":1,"Street":1,"City":1})
#    print "dgeoinfo.mongoausgabe",
#    if debugmode:
#        pprint (ausgabe)

    if False:
        #Kopie der Zeien zum Debugen
        posts = mongoconnect()
        mongorequest = ({"latitude":lati,"longitude":longi,"Housenumber": {"$exists": True}},{"Housenumber":1,"Street":1,"City":1})
        print "\n\nMongorequest::",mongorequest
        ausgabe = posts.find_one({"latitude":lati,"longitude":longi,"Housenumber": {"$exists": True}},{"Housenumber":1,"Street":1,"City":1})
        if debugmode:
#            print "dgeoinfo.geofrom:Mongo",ausgabe['City'],ausgabe['Street'],
            pprint (ausgabe)
#            sys.exit(2)
            if ausgabe['City']:
                print "dgeoinfo.mongoCity:",ausgabe['City'],
            if ausgabe['Street']:
                print "dgeoinfo.mongoStreet:",ausgabe['Street'],
            if ausgabe['Housenumber']:
                print "dgeoinfo.mongoHousenumber:",ausgabe['Housenumber'],

    try:
        ausgabe = posts.find_one({"latitude":args['lati'],"longitude":args['longi'],"Housenumber": {"$exists": True}},{"Housenumber":1,"Street":1,"City":1})
        if debugmode:
            print "dgeoinfo.geofrom:Mongo",ausgabe['City'],ausgabe['Street']
            if ausgabe['City']:
                print "dgeoinfo.mongoCity:",ausgabe['City']
            if ausgabe['Street']:
                print "dgeoinfo.mongoStreet:",ausgabe['Street']
            if ausgabe['Housenumber']:
                print "dgeoinfo.mongoHousenumber:",ausgabe['Housenumber']


        if ausgabe['City'] and ausgabe['Street'] and ausgabe['Housenumber']:
            print "dgeoinfo.geofrom:Mongo::",ausgabe['City'],ausgabe['Street']
            return(ausgabe)
    except:
        try:
            posts = mongoconnect()
            ausgabe = posts.find_one({"latitude":args['lati'],"longitude":args['longi'],"Housenumber": {"$exists": True}},{"Housenumber":1,"Street":1,"City":1})
            if ausgabe['City'] and ausgabe['Street'] and ausgabe['Housenumber']:
                print "dgeoinfo.geofrom:Mongo2::",ausgabe['City'],ausgabe['Street']
                return(ausgabe)
        except:
            print"",
#    sys.exit(0)

    try:
        gmaps = googlemaps.Client(key=args['glogin'])
        georesult = gmaps.reverse_geocode((args['lati'], args['longi']))
        googlerequestcount += 1
        i = 0
        city=""
        street=""
        housenumber="-"
        zip=""
        country=""
        for geocomp in georesult[0]['address_components']:
        	for geotype in geocomp['types']:
        		if geotype == 'locality':
        			city = georesult[0]['address_components'][i]['long_name']
        		if geotype == 'route':
        			street = georesult[0]['address_components'][i]['long_name']
        		if geotype == 'street_number':
        			housenumber = georesult[0]['address_components'][i]['long_name']
        		if geotype == 'postal_code':
        			zip = georesult[0]['address_components'][i]['long_name']
        		if geotype == 'country':
        			country = georesult[0]['address_components'][i]['long_name']
        	i+=1

        geo = {
            'longitude':args['longi'],
            'latitude':args['lati'],
        	'City':city,
        	'Street':street,
        	'Housenumber':housenumber,
        	'zip':zip,
        	'Country':country,
            'geoinfoonly':True
        }
        print "dgeoinfo.geofrom:google",geo['City'],geo['Street'],"HN:",geo['Housenumber'],type(geo['Housenumber'])
    	post_id = posts.insert_one(geo)
    	if debugmode:
    		print "dgeoinfo.Eintrag in MongoDB"
    		print "dgeoinfo.Rückgabewert nach Eintrag post_id:",post_id

        return(geo)
    except:
        print "Fehler in der geoauflösung"



def mongocheck():
    global googlerequestcount
    global mongoupdates
    print "mongocheck.start"
    try:
        posts = mongoconnect()
    except:
        print "Fehler im DB connect"
#        sys.exit(1)

    repairs = posts.find({"City":{"$exists":False}},{"longitude":1,"latitude":1,"messZeit":1,"City":1}).count()
    ausgabe = posts.find({"City":{"$exists":False}},{"_id":1,"longitude":1,"latitude":1,"messZeit":1,"City":1}).limit(100)
    print "Zu korrigierende Werte:",repairs

    for dd in ausgabe:
        print "\n\nmongocheck.for_ausgabe:",
        geoatri={
            'longi':dd['longitude'],
            'lati':dd['latitude'],
            'glogin':glogin,
            'posts':posts
        }
        geoinfo = dgeoinfo(geoatri)
#        geoinfo = dgeoinfo(dd["longitude"],dd["latitude"],glogin,posts)
        try:
            if mongoupdate:
                posts.update_one({'_id': dd['_id']}, {"$set": { "Street": geoinfo["Street"], "City": geoinfo["City"], "Housenumber": geoinfo["Housenumber"], "updated": True}})
                mongoupdates += 1
        except:
            print "!!!!Fehlerausgabe"


def mysqlcheck():
    global googlerequestcount
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



###start Hauptprogram


debugmode = False
swmongocheck = False
swmysqlcheck = False
mongoupdate = True
mysqlupdate = True
askgoogle = False
data = []
mysqllimit = 10
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
glogin = getTeslaconf['glogin']


startzeit = time.time()
if swmongocheck:
    print "\n\nmongocheck"
    mongocheck()

if swmysqlcheck:
    print "\n\nmysqlcheck"
    mysqlcheck()

print "Googleanfragen:", googlerequestcount
print "mysqljobs:",mysqljobs
print "monogupdates:",mongoupdates
print "mysqlupdates",mysqlupdates
print "Laufzeit: ", time.time() - startzeit

print "-----ENDE-------------"
#pprint (ausgabe)
