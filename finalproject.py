import unittest
import itertools
import collections
import tweepy
import twitter_info 
import sqlite3
import requests
import json 
import pickle
#Part one: import the nessesary modules, set up tweepy authorization, make sure to have twitter_info in same place as this file!
consumer_key = twitter_info.consumer_key
consumer_secret = twitter_info.consumer_secret
access_token = twitter_info.access_token
access_token_secret = twitter_info.access_token_secret
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

CACHE_FNAME = "Final-Project.json"  #creating the Cache 
############### Cache Set up########################
# Part 2: Cache set up, try except block, if inside, use cache data, if not, create an empty dictionary in which data will be appended to it
try:
	cache_file = open(CACHE_FNAME,'r')
	cache_contents = cache_file.read()
	cache_file.close()
	CACHE_DICTION = json.loads(cache_contents)
except:
	CACHE_DICTION = {}
################### Movie data fetching #############
# Part three: function creationa and data fetch.  Create 3 functions each to grab movie data, twitter data about an actor via from the movie data and 
# a user function that gets any other users in the neighborhood data.  Each function must cache the data so this program can run offline for this use ur Cache set up!
#make sure to know ur inputs of every function!!!!!

def omdb_data(title):
	if title in CACHE_DICTION: 
		results=CACHE_DICTION[title]
		return results 
	else: 
		basekey="http://omdbapi.com/"
		params={"t":"{}".format(title)}
		results=requests.get(basekey,params=params)  #response object meta data 
		results=results.text #make this data readable 
		results=json.loads(results) #load to json dict 
		CACHE_DICTION[title]=results
		f=open(CACHE_FNAME, "w")
		f.write(json.dumps(CACHE_DICTION)) #dump the dict into text form
		f.close()
		return CACHE_DICTION[title]
################## twiiter Data Fetching#################
def twitter_data(a): #an instance of the class class get inputed to retrieve data about a given actor
	if a in CACHE_DICTION:
		results=CACHE_DICTION[a]
		return results
	else: 
		results=api.search(q=a)
		CACHE_DICTION[a]=results
		f=open(CACHE_FNAME, "w")
		f.write(json.dumps(CACHE_DICTION))
		f.close()
		return CACHE_DICTION[a]
		
def user_data(q): #this function will get called within the user's tuple when looking for information about user mentions to create a twitter neighborhood! 
	if q in CACHE_DICTION:
		c=CACHE_DICTION[q]
		return c 
	else:
		c=api.get_user(q)
		CACHE_DICTION[q]=c
		f=open(CACHE_FNAME, "w")
		f.write(json.dumps(CACHE_DICTION))
		f.close()
		return CACHE_DICTION[q]
				
#######################CLASSES##############################
# Part 4: create some classes to pick apart the data! 
# create a method long or short that tells us if the movie is above 2 hrs long and antother how_old method that tells us years from creation
# within the class tweet make sure to invoke ur cache function within the method so that user_mentions can be found! 
class Movie():
	def __init__(self,x): 
		self.title=x["Title"]
		self.director=x["Director"]
		self.actors=x["Actors"].split(",")
		self.imdb_rating= x["imdbRating"]
		self.languages= x["Language"]
		self.metascore= x["Metascore"]
		self.imdb_id= x["imdbID"]
		self.runtime= x["Runtime"]
		self.year_created=x["Year"]
		self.awards=x["Awards"]
		self.genre=x["Genre"]
		self.top_actor=x["Actors"].split(",")[0]
	def long_or_short(self): 
		self.runtime=self.runtime.strip("min")
		self.runtime=self.runtime.strip("")
		self.runtime=int(self.runtime)
		if self.runtime>120:
			return "this film is rather long"
		if self.runtime<120: 
			return "this film is rather short"
		if self.runtime==120:
			return "this film is average in length"
	def how_old(self):
		self.year_created=int(self.year_created)
		how_old=2017-self.year_created 
		return how_old
	def tup(self): #important for data base intagration 
		tup=(self.imdb_id,self.title,self.director,self.imdb_rating,self.top_actor, self.languages, self.genre, self.runtime, self.year_created, self.metascore,self.awards,self.how_old(),self.long_or_short())  
		return tup 


class Tweet():
	def __init__(self,x):
		self.text=x["text"]
		self.tweet_id=int(x["id_str"])
		self.imdb_id=" "
		self.language=x["lang"]
		self.user_id=x["user"]["id_str"]
		self.location=x["user"]["time_zone"] #choose time zone becasue location is user inputted which can be funky, still gives us a general idea of where the user is from
		self.num_favs=int(x["favorite_count"])
		self.num_rwts=int(x["retweet_count"])
		self.users=x["entities"]["user_mentions"]
		self.user_screen_name=x["user"]["screen_name"]
		self.user_fav_count=int(x["user"]["favourites_count"])
		self.user_followers=int(x["user"]["followers_count"])

	def tweets_tup(self): 
		tup=(self.tweet_id,self.user_id,self.imdb_id,self.text,self.num_favs,self.num_rwts,self.language)  #movie search this tweet came from?? 
		return tup 
	def tweeted_user_info(self): #this tuple and mentions_users_list will be merged together must make sure no duplicates are entered 
			tup=(self.user_id,self.user_screen_name,self.user_fav_count,self.user_followers,self.imdb_id,self.location)
			return tup
	def mentions_users_list(self): 
		for x in self.users:
			tempScreenName=x["screen_name"]
			cache=user_data(tempScreenName)
			tup=(cache["id_str"],cache["screen_name"],cache["favourites_count"], cache["followers_count"],self.imdb_id,cache["time_zone"])
			return tup

############################# list creation, looping making sure tuples set for class creation #############################	
#part 5 make list outside of teh classes that will be easily put into the db! make sure to merge user_mentions and list_of_users to creat one big neighborhood.
#make sure to only append data into list_of_mentions if not none! dont care for none values. 
mov_lst=["happy gilmore","good will hunting","miracle"]
Movie_data=[omdb_data(x) for x in mov_lst]


list_of_movie_instances=[Movie(x) for x in Movie_data] #simplify 
# for x in Movie_data:
# 	inst=Movie(x)
# 	list_of_movie_instances.append(inst)

list_of_movie_tups=[x.tup() for x in list_of_movie_instances] #simplify 
# for x in list_of_movie_instances:
# 	tup=x.tup()
# 	list_of_movie_tups.append(tup)
# print (list_of_tups)

list_of_tweets=[]
list_of_users=[]
list_of_mentions=[]
for movie_inst in list_of_movie_instances: 
	actor=movie_inst.top_actor
	twitter_data_=twitter_data(actor)
	# data_in_statuses=twitter_data_["statuses"]
	for x in twitter_data_["statuses"]:
		twitter_inst=Tweet(x)
		twitter_inst.imdb_id=movie_inst.imdb_id

		tup=twitter_inst.tweets_tup()
		list_of_tweets.append(tup)

		tweet_user_tup=twitter_inst.tweeted_user_info()
		list_of_users.append(tweet_user_tup)


		tweet_user_mentions=twitter_inst.mentions_users_list() #no nones in list 

		# print(tweet_user_mentions)
		# print('\n')
		
		if tweet_user_mentions is None:
			pass
		else:
			list_of_mentions.append(tweet_user_mentions)

for items in list_of_mentions: #making sure this user_id does not match others user_id so no dupslicates
	if items[0] not in list_of_users:
		list_of_users.append(items)
# print(list_of_users)
# print('\n')
########################### Data Base Creation ##################
# part 6: data base set up, make sure to have correct type  with given inout and remember to execute and connect.commit() after each table creation
# remember to use insert of ignore to surpass the duplication of data import issue in sql!!!!!

connect=sqlite3.connect("movie_tweet.db")
cur=connect.cursor()
cur.execute('DROP TABLE IF EXISTS Movies')
cur.execute('DROP TABLE IF EXISTS Tweets')
cur.execute('DROP TABLE IF EXISTS Users')

new_table='CREATE TABLE IF NOT EXISTS '
new_table+='Movies (imdb_id TEXT PRIMARY KEY,title TEXT ,director TEXT ,imdb_rating TEXT ,top_actor TEXT ,languages TEXT , genre TEXT , runtime TEXT , year_created TEXT , metascore TEXT, awards TEXT , how_old INTEGER ,long_or_short TEXT)' 
cur.execute(new_table)

new_table='CREATE TABLE IF NOT EXISTS '
new_table+='Tweets (tweet_id INTEGER PRIMARY KEY, user_id TEXT, imdb_id TEXT , Tweet_text TEXT, num_favs INTEGER, num_rwts INTEGER, Language Text)'
cur.execute(new_table)

new_table='CREATE TABLE IF NOT EXISTS '
new_table+='Users (user_id TEXT PRIMARY KEY, user_screen_name TEXT, fav_count INTEGER, followers_count INTEGER, imdb_id TEXT, Location Text)'
cur.execute(new_table)

connect.commit()

statement_1='INSERT INTO Movies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'
for items in list_of_movie_tups:
	cur.execute(statement_1,items)
connect.commit()
statement_2='INSERT INTO Tweets VALUES (?,?,?,?,?,?,?)'
for items in list_of_tweets:
	cur.execute(statement_2,items)
connect.commit()
statement_3='INSERT OR IGNORE INTO Users Values (?,?,?,?,?,?)' #important statement that allows duplicates 
for items in list_of_users: 
	cur.execute(statement_3,items)
connect.commit()

############ SQL queries and export to csv ###############
# what would be intresting? # brain strom ideas to make sql queries to draw correlations in the given data.  Make sure to export those queries to csv 
# to create a file output.  remember to use the proper amount of data processing mechanisms in order to fufull the project req. 

#######################################

sql_1='SELECT Movies.top_actor,Users.location FROM Movies INNER JOIN Users on Movies.imdb_id =Users.imdb_id'
cur.execute(sql_1)
actor_and_location=cur.fetchall()


dictionary_of_actors={}


for actor,location in actor_and_location: 
	if location==None:
		continue 
	#check to see if actor dictionary exists in dictionary
	if actor not in dictionary_of_actors:
		dictionary_of_locations={}
		dictionary_of_actors[actor]= dictionary_of_locations
	
	#check to see if location is seen before in dictionary
	if location not in dictionary_of_actors[actor]:
		dictionary_of_locations[location]=1
	
	if location in dictionary_of_locations:
		dictionary_of_locations[location] +=1


most_common_dict={}
for key in dictionary_of_actors:
	each_actor=dictionary_of_actors[key]
	current_max = 0
	name_out = ''
	for x_location in each_actor:
		if each_actor[x_location]>current_max:
			current_max = each_actor[x_location]
			name_out = x_location

	most_common_dict[key]=name_out

	
				
sql_2='SELECT Movies.top_actor,Tweets.Tweet_text FROM Movies INNER JOIN Tweets on Movies.imdb_id=Tweets.imdb_id where language="en"'
cur.execute(sql_2)
actor_and_tweet=cur.fetchall()
# print (actor_and_tweet)

d=collections.defaultdict(list)
for k,v in actor_and_tweet:
	d[k].append(v)
merged_dict=dict(d)



list_of_common_english_words=["rt","the", "of", "and", "a", "to", "in", "is", "you", "that", "it", "he", "was", "for", "on", "are", "as", "with", "his", "they", "I", "at", "be", "this", "have", "from", "or", "one", "had", "by", "word"]

new_dict={}
# print(len(merged_dict["Kurt Russell"]))
for keys in merged_dict:
	count=collections.Counter()
	for tweets in merged_dict[keys]: 
		# split_words=tweets.split()
		count.update(word.lower() for word in tweets.split())
		count_dict=dict(count)
		list_of_tuples=list(count_dict.items())
	
	counter=''
	new_list=[]
	for tups in list_of_tuples:
		words=tups[0].lower()
		if tups in new_list:
		
			continue
		if words=="RT":
		
			continue
		if words in list_of_common_english_words:
			continue

		if words!=counter:
				
				if tups[1]>1:
					new_list.append(tups)
					counter=words
	 # print (new_list)
	new_list.sort(key=lambda tup: tup[1], reverse=True)		
	# print("List 1: ", new_list, '\n')
	new_dict[keys]=new_list
	# print (new_dict)


sql_3='SELECT Movies.top_actor,Users.user_screen_name FROM Movies INNER JOIN Users on Movies.imdb_id=Users.imdb_id '
cur.execute(sql_3)
actor_and_screenname=cur.fetchall()

a_s= collections.defaultdict(list)
for k,v in actor_and_screenname:
	a_s[k].append(v)
actors_and_screename=dict(a_s)
# print (actors_and_screename)

#######################OUTFILE CREATION#################################

outfile=open("TweetMovieActorSummary.txt","w")
outfile.write("Top Movie Actor and Twitter Data! By Cam Newman")
outfile.write("\n\n")
outfile.write("This file provides social media information about the top actors in the movies: Miracle, Good Will Hunting and Happy Gilmore. The data below provides a list of twitter users that my data was gathered from. Below shows the most frequent location (time zone) that people tweet about each actor from(depends on sample). Additionally, this page presents recurring words/items/links/mentions/emojis (more than once) throughout the tweets excluding the top 30 most common words in the english language or 'RT'. It is important to mention the values can change (if delete cache and re-run).") 
outfile.write("\n\n")
for keys in actors_and_screename:
	outfile.write(" For Actor: " '{}' " These tweet samples come from these users screen names:".format(keys))
	outfile.write("\n\n")
	for items in actors_and_screename[keys]:
		outfile.write(items)
		outfile.write("\n\n")
outfile.write("\n\n")
outfile.write("Location")
outfile.write("\n\n")
for keys in most_common_dict:
	a=most_common_dict[keys]
	outfile.write("Actor: " '{}' " the most common location of tweets about this actor are in(the) '{}' ".format(keys,a))
	outfile.write("\n\n")
outfile.write("\n\n")
outfile.write("Word Frequency")
outfile.write("\n\n")
for keys in new_dict:
	outfile.write("Actor: '{}' The most common words in the tweets are:".format(keys))
	outfile.write('\n\n')
	for d,k in new_dict[keys]:
		outfile.write("* '{}' , was used, '{}' times".format(d,k))
		outfile.write('\n\n')	


				
################## TEST CASES#################
#write test cases for this project... make sure to fufill the requiremnts in the directions for the amount of test you need for each functiona and class
class tests(unittest.TestCase):

	def test_01(self): #testing function twitter_data
		self.assertEqual(type(twitter_data("matt damon")), type(omdb_data("good will hunting")))  #checkng to see if data we are compiling is of the same type
	def test_02(self): #testing function omdb data 
		self.assertEqual(type(omdb_data("Miracle")),dict)
	def test_03(self): #testing function user_data
		for x in twitter_data_["statuses"]:
			for y in x["entities"]["user_mentions"]:
				self.assertEqual(type(user_data(y["screen_name"])),dict)
	def test_04(self): #testing instance of tweet class 
		for x in twitter_data_["statuses"]:
			c=Tweet(x) #checking type of instance varaible 
			self.assertEqual(type(c.text), str)
	def test_05(self): #testing for tweets_tup method in tweet class 
		for x in twitter_data_["statuses"]:
			c=Tweet(x)
			self.assertEqual(len(c.tweets_tup()),7)
	def test_06(self): #testing method tweeted_user_info
		for x in twitter_data_["statuses"]:
			c=Tweet(x)
			self.assertEqual(type(c.tweeted_user_info()),tuple)
	def test_07(self): #testing method method_users_list sometime you get none, thats ok! we can take those out later!
		for x in twitter_data_["statuses"]:
			c=Tweet(x)
			if c.mentions_users_list()==None:
				continue
			else:
				self.assertEqual(type(c.mentions_users_list()),tuple)
	def test_08(self): #testing var list_of_movie_instances 
		self.assertEqual(len(list_of_movie_instances),3) #making sure length of instances is equal to 3 
	def test_09(self): #testing list of movie_tups
		for x in list_of_movie_tups: 
			self.assertEqual(type(x),tuple) #checking if each item in the list of combind data isa tuple, important for importing data into db.
	def test_10(self): #testing_of_users
		for x in list_of_users:
			self.assertEqual(type(x),tuple) 
	def test_11(self): #testing movie class 
		for x in Movie_data:
			c=Movie(x)
			d=c.top_actor
			self.assertEqual(type(d),str)
	def test_12(self):
		self.assertEqual(len(Movie_data),3) #data retreived is in cache and equals it 
	def test_13(self): #testing tup movie method 
		for x in Movie_data:
			C=Movie(x)
			self.assertEqual(type(C.long_or_short()), str)
			# see if famous methods returns a string type
	def test_14(self): #testing tup moive mothod 
		for x in Movie_data:
			c=Movie(x)
			self.assertEqual(type(c.tup()),tuple)
	def test_15(self): #testing if the length of new_dict which is the product of finding the most common words (occured more than 5 times) is 3 
		self.assertEqual(len(new_dict),3)
	def test_16(self): #testing if location value is a string for location data manipulation
		self.assertEqual(type(most_common_dict["Adam Sandler"]),str)
	def test_17(self):
		conn = sqlite3.connect('movie_tweet.db') #testing users table
		cur = conn.cursor()
		cur.execute('SELECT * FROM Users');
		result = cur.fetchall()
		self.assertTrue(len(result[0])==6)
		conn.close()
	def test_18(self):
		conn = sqlite3.connect('movie_tweet.db') #testing movies table 
		cur = conn.cursor()
		cur.execute('SELECT * FROM Movies');
		result = cur.fetchall()
		self.assertTrue(len(result[0])==13)
		conn.close()
	def test_19(self):
		conn = sqlite3.connect('movie_tweet.db') #testing tweets table 
		cur = conn.cursor()
		cur.execute('SELECT * FROM Tweets');
		result = cur.fetchall()
		self.assertTrue(len(result[0])==7)
		conn.close() 
	def test_20(self): #testing strucutre actors and screename result of data manip
		self.assertEqual(len(actors_and_screename),3)
	def test_21(self): #testing output of sql statemnts 
		self.assertEqual(type(actor_and_location),list)
	def test_22(self):#test output of sql statemnts 
		self.assertEqual(type(actor_and_tweet),list)
	def test_23(self):#testing output of sql statemnts 
		self.assertEqual(type(actor_and_screenname),list)
unittest.main(verbosity=2)

