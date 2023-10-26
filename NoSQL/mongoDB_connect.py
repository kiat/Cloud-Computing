# run 
# docker pull mongo
# docker run -d -p 27017:27017  mongo

# docker exec -it ef590dc4579a   /bin/bash
# ef590dc4579a is the ID of the docker container 

# after you are login to the container 
# run 
# mongosh


# https://www.mongodb.com/languages/python

# mongosh mongo shell
# https://www.mongodb.com/docs/mongodb-shell/run-commands/

# show databases
# use database: use students
# search and show the content of a datbase: db.getCollection("students").find()
# delete databse: db.dropDatabase()

from pymongo import MongoClient
import pymongo


def get_database():

    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    # CONNECTION_STRING = "mongodb+srv://<username>:<password>@<cluster-name>.mongodb.net/myFirstDatabase"
    CONNECTION_STRING = "mongodb://127.0.0.1:27017/?directConnection=true"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['students']

# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":

    # Get the database
    dbname = get_database()

    # print(str(dbname))

    collection_name = dbname["students"]

    student_1 = {
      "student_id": "U1IT00001",
      "firstname": "John",
      "lastname": "Doe",
      "class_year": 2017,
      "courses": [
        {
          "course_id": "CS378",
          "title": "Cloud Computing",
          "unit": 4,
          "tuition": 1500,
          "sections": [
            5051,
            5051
          ]
        },
        {
          "course_id": "CS108",
          "title": "Software Systems",
          "unit": 4,
          "tuition": 1100,
          "sections": [
            4011,
            5021
          ]
        }
      ]
    }


    student_2 = {
      "student_id": "U1IT00002",
      "firstname": "Mike",
      "lastname": "Smith",
      "class_year": 2018,
      "courses": [
        {
          "course_id": "CS378",
          "title": "Cloud Computing",
          "unit": 4,
          "tuition": 1500,
          "sections": [
            5051,
            5051
          ]
        },
        {
          "course_id": "CS328",
          "title": "Data Structutes",
          "unit": 4,
          "tuition": 1150,
          "sections": [
            6051,
            7052
          ]
        }
      ]
    }



    collection_name.insert_many([student_1, student_2])

###########################################################
# Reading the data back.

    dbname = get_database()


    # Create a new collection
    collection_name = dbname["students"]

    students = collection_name.find()

    for student in students:
        # This does not give a very readable output
        print(student)
