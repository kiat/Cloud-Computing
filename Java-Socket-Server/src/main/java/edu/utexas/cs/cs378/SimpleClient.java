package edu.utexas.cs.cs378;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class SimpleClient {

    static public int portNumber = 33333;
    static public String serverAddress = "localhost";


    public static void main(String[] args) {


        if (args.length >= 1) {
            System.err.println("Usage: SimpleClient <hostname> <port number> ");
            serverAddress = args[0];
            portNumber = Integer.parseInt(args[1]);
        }


        try (Socket socket = new Socket(serverAddress, portNumber)) {

            System.out.println("Connected to server.");

            // Do this forever until user types "exit"
            String flag ="";

            while (!flag.equals("exit")) {

                // Console system in
                Scanner input = new Scanner(System.in);
                System.out.print("Enter your message: ");
                String message = input.nextLine();

                // Send data to the server
                PrintWriter outputSocket = new PrintWriter(socket.getOutputStream(), true);
                outputSocket.println(message);

                // Receive data from the server
                BufferedReader inputSocket = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String response = inputSocket.readLine();

                System.out.println("Server says: " + response);

//                // set the flag to be able to exit.
//                flag = message;

                // Check for client disconnect
                if (message.equalsIgnoreCase("exit")) {
                    outputSocket.println("Goodbye! Disconnecting...");
                    socket.close();
                    System.exit(0);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}