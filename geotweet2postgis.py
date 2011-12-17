from tweepy.streaming import StreamListener, Stream
from tweepy.auth import BasicAuthHandler
from psycopg2 import connect
from optparse import OptionParser
from pdb import set_trace

p = OptionParser()
p.add_option('-g', '--geo', dest='geo', type='string',
             help='The geographic area to fetch tweets from, left,bot,right,top',
             metavar='GEO_SPEC', default='-122.75,36.8,-121.75,37.8')
p.add_option('-t', '--table', dest='table', type='string',
                  help='the table to store data in',
                  metavar='TABLE', default='geotweets')
p.add_option('-d', '--dsn', dest='dsn', type='string',
                  help='database dsn, as documented at http://initd.org/psycopg/docs/module.html#psycopg2.connect',
                  metavar='DSN', default='dbname=tweets')

opts, args = p.parse_args()

class PGSQLListener(StreamListener):
    def on_status(self, status):
        # Connect to the DB
        conn = connect(opts.dsn)
        cursor = conn.cursor()

        # Don't know how this happens
        if status.geo == None:
            print 'No geo information'
            return

        if status.geo['type'] != 'Point':
            print 'Geo is of type ' + status.geo['type']
            return

        # Insert the status into the DB.

        wkt = 'SRID=4326;POINT(%s %s)' % (status.geo['coordinates'][1], status.geo['coordinates'][0])

        cursor.execute('INSERT INTO ' + opts.table + 
                       ' (screenname, tweet, the_geom) VALUES ' +
                       '(%s, %s, ST_GeomFromEWKT(%s))',
                       (status.user.screen_name,
                        status.text,
                        wkt
                        ))
        conn.commit()

listener = PGSQLListener()
auth = BasicAuthHandler(args[0], args[1])
s = Stream(auth, listener, secure=True)
#s.filter(track="foothill college")
s.filter(locations=[float(i) for i in opts.geo.split(',')])
