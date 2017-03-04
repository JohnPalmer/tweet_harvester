import local_secrets
import tweepy
import dataset
from sqlalchemy.exc import ProgrammingError
import json

auth = tweepy.OAuthHandler(local_secrets.TWITTER_APP_KEY, local_secrets.TWITTER_APP_SECRET)

auth.set_access_token(local_secrets.TWITTER_KEY, local_secrets.TWITTER_SECRET)

api = tweepy.API(auth)

db = dataset.connect(local_secrets.CONNECTION_STRING)

class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        description = status.user.description
        loc = status.user.location
        text = status.text
        coords = status.coordinates
        coords_lat = None
        coords_lon = None
        geo = status.geo
        geo_lat = None
        geo_lon = None
        name = status.user.screen_name
        user_created = status.user.created_at
        followers = status.user.followers_count
        id_str = status.id_str
        created = status.created_at
        retweets = status.retweet_count
        if geo is not None:
            if geo['coordinates'] is not None:
                geo_lat = geo['coordinates'][0]
                geo_lon = geo['coordinates'][1]
            geo = json.dumps(geo)            
        if coords is not None:
            if coords['coordinates'] is not None:
                    coords_lat = coords['coordinates'][1]
                    coords_lon = coords['coordinates'][0]
            coords = json.dumps(coords)
        table = db[local_secrets.TABLE_NAME]
        try:
            table.insert(dict(
                user_description=description,
                user_location=loc,
                coordinates=coords,
                coordinaes_lat=coords_lat,
                coordinates_lon=coords_lon,
                text=text,
                geo=geo,
                geo_lat=geo_lat,
                geo_lon=geo_lon,
                user_name=name,
                user_created=user_created,
                user_followers=followers,
                id_str=id_str,
                created=created,
                retweet_count=retweets,
            ))
        except ProgrammingError as err:
            print(err)
        return True
    def on_error(self, status_code):
        if status_code == 420:
            return False

while True:
    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener, timeout=60)
    stream.filter(locations=[2.052498,41.320881,2.228356,41.461511])
    
    try:
        stream.userstream()

    except Exception as e:
        print "Error. Restarting Stream.... Error: "
        print e.__doc__
        print e.message


