# from __future__ import absolute_import, print_function

from curses.ascii import NUL
import socket
import json
import time
import sys
import bz2       


def main(filename):
   s = socket.socket()
   TCP_IP = "localhost"
   TCP_PORT = 9009

   s.bind((TCP_IP, TCP_PORT))
   s.listen(1)


   print("Wait here for TCP connection ...")

   conn, addr = s.accept()

   print("Connected, lets go get send some data.")
   counter = 0



   # Open file
   with open(filename, "r") as fileHandler:

       # Read each line in loop
        for line in fileHandler:
           counter += 1

           # Sleep for some time to not send too much data!   
           time.sleep(0.10)
           
           # as an example we send here just the line text.
           tweet_text = line.encode('utf-8')
           conn.send(tweet_text)
           
           if(counter % 100 == 0):
               print(counter)

        s.close()
            
            
if __name__ == "__main__":

    if len(sys.argv) < 2 :
        print("Usage: send_text_from_file.py <file> ", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
