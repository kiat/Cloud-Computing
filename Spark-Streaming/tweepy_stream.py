# Importing Tweepy and time
import tweepy
import time
import json
import socket




# Bot searches for tweets containing certain keywords
class MyStream(tweepy.StreamingClient):

    def __init__(self, bearer_token, csocket):
      super().__init__(bearer_token)
      self.client_socket = csocket

    # This function gets called when the stream is working
    def on_connect(self):
        print("Connected")


    # This function gets called when a tweet passes the stream
    def on_tweet(self, tweet):

        if tweet.referenced_tweets == None:

            try:
                msg = json.dumps(tweet.data)
                # This is a large JSON object for tweet
                print(msg)

                # tweet_text = msg['text'].encode('utf-8')
                # screen_name = msg['user']['screen_name'].encode('utf-8')
                # print(tweet_text)

                # print(msg['coordinates'])
                # print(msg['place'])
                # print(msg['geo'])

                self.client_socket.send(tweet.text.encode('utf-8'))

                return True
            except BaseException as e:
                print("Error on_data: %s" % str(e))
            return True

        
################################################################

def sendData(csocket, bearer_token, search_terms):

    # Creating Stream object
    stream = MyStream(bearer_token=bearer_token, csocket=csocket)

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
    stream.filter(tweet_fields=["referenced_tweets","author_id","created_at"])

    # place_fields=["full_name", "id", "contained_within",  "country", "country_code", "geo", "name",  "place_type" ]
    # place_fields=["geo"]

    # stream.filter(tweet_fields=["referenced_tweets","author_id","created_at"], user_fields=["created_at"],\
    #             expansions=["author_id", "referenced_tweets.id", "entities.mentions.username", "in_reply_to_user_id",\
    #                         "referenced_tweets.id.author_id"])



##################################################
##################################################
def main():

    # Credentials (INSERT YOUR KEYS AND TOKENS IN THE STRINGS BELOW)
    bearer_token = r"AAAAAAAAAAAAAAAAAAAA ADD YOUR OWN KEY HERE!"
    
    search_terms = ["data", "programming", "coding"]
    # search_terms = ["data"]

    s = socket.socket()
    TCP_IP = "localhost"
    TCP_PORT = 9009

    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)

    print("Wait here for TCP connection ...")

    conn, addr = s.accept()

    print("Connected, lets go get tweets.")
    sendData(conn, bearer_token, search_terms)


if __name__ == "__main__":
    main()
