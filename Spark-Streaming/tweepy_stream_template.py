# Importing Tweepy and time
import tweepy
import time
import json


# Credentials (INSERT YOUR KEYS AND TOKENS IN THE STRINGS BELOW)
api_key = "ZjDVw1USXVwS95O6Wd8lOCjgI"
api_secret = "gOvDonoL7PpoCZHsLtQMnwukyCdraVmuw2dc5ZxIlUZZ9b79tl"
bearer_token = r"AAAAAAAAAAAAAAAAAAAAAFqvQgAAAAAAlGMaCd1C%2F5iS9x27QdeGjU9Cqb8%3D7uVYC0FSeI3mtC88oQcaMCXSxYouvWplML8bd1qNnbtxZuCkh0"
access_token = "54719250-Tm63sSHZwO3uuJBs8XGqSdzdLZ7tFaHw8j1WYrA5I"
access_token_secret = "6bDCLinYRpghKQQh37W6w747RO20G4Nsed4KpGlzm5bgD"

# Gainaing access and connecting to Twitter API using Credentials
client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

search_terms = ["data", "programming", "coding"]
# search_terms = ["data"]

# Bot searches for tweets containing certain keywords
class MyStream(tweepy.StreamingClient):

    # This function gets called when the stream is working
    def on_connect(self):
        print("Connected")


    # This function gets called when a tweet passes the stream
    def on_tweet(self, tweet):

        # Displaying tweet in console
        if tweet.referenced_tweets == None:
            msg = json.dumps(tweet.data)
            
            print(msg)
            # print(tweet)
            # print(tweet.text)
            # client.like(tweet.id)

            # Delay between tweets
            # time.sleep(0.1)
        

# Creating Stream object
stream = MyStream(bearer_token=bearer_token)

# Adding terms to search rules
# It's important to know that these rules don't get deleted when you stop the
# program, so you'd need to use stream.get_rules() and stream.delete_rules()
# to change them, or you can use the optional parameter to stream.add_rules()
# called dry_run (set it to True, and the rules will get deleted after the bot
# stopped running).
for term in search_terms:
    stream.add_rules(tweepy.StreamRule(term))
    

# print(stream.get_rules())
# Starting stream, 
# stream.filter(tweet_fields=["referenced_tweets","author_id","created_at"], \

# place_fields=["full_name", "id", "contained_within",  "country", "country_code", "geo", "name",  "place_type" ]
# place_fields=["geo"]


    
stream.filter(tweet_fields=["referenced_tweets","author_id","created_at"], user_fields=["created_at"],\
              expansions=["author_id", "referenced_tweets.id", "entities.mentions.username", "in_reply_to_user_id",\
                          "referenced_tweets.id.author_id"])


