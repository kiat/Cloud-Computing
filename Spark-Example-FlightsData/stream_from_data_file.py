# from __future__ import absolute_import, print_function

from curses.ascii import NUL
import socket
import json
import time
import sys



def main(filename):
   s = socket.socket()
   TCP_IP = "localhost"
   TCP_PORT = 9009

   s.bind((TCP_IP, TCP_PORT))
   s.listen(1)


   print("Wait here for TCP connection ...")

   conn, addr = s.accept()

   print("Connected, sending data over the stream.")

   counter = 0



   # Open file
   with open(filename, "r") as fileHandler:

       # Read each line in loop
        for line in fileHandler:
           counter += 1

           # Sleep for some time
           time.sleep(0.05)
           conn.send(line.encode('utf-8'))

           if(counter % 100 == 0):
               print(counter)

        s.close()


if __name__ == "__main__":

    if len(sys.argv) < 2 :
        print("Usage: Tweets_Send_From_File.py <file> ", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
