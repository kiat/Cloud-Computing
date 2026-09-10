package edu.utexas.cs.cs378;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class SimpleServer {

    static public int portNumber = 33333;


    public static void main(String[] args) {


        if (args.length > 0) {
            System.err.println("Usage: MainServer <port number>");
            portNumber = Integer.parseInt(args[0]);
        }


        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {

            System.out.println("Server started.");
            System.out.println("Waiting for a client on port " + portNumber + "...");


            // Wait for a client to connect
            Socket socket = serverSocket.accept();

            System.out.println("Client connected: " + socket.getInetAddress());

            // Receive data from the client
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Send data to the client
            PrintWriter output = new PrintWriter(socket.getOutputStream(), true);


            // Do this forever until user types "exit"
            String flag = "";
            while (!flag.equals("go down")) {
                String message = input.readLine();
                System.out.println("Client says: " + message);

                // Send response
                output.println("Hello from the server!");

                // Check for client disconnect
                if (message.equalsIgnoreCase("go down")) {
                    output.println("Goodbye! Disconnecting...");
                    socket.close();
                    break;
                }


            }



        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}