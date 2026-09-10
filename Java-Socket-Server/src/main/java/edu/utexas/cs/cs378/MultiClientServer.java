package edu.utexas.cs.cs378;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class MultiClientServer {

    static int portNumber = 33333;

    public static void main(String[] args) {

        // Get port number from command line
        if (args.length > 0) {
            portNumber = Integer.parseInt(args[0]);
        }

        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {

            System.out.println("Server started.");
            System.out.println("Waiting for clients on port " + portNumber + "...");

            // Continuously accept new clients
            while (true) {

                Socket socket = serverSocket.accept();

                System.out.println("Client connected: " + socket.getInetAddress());

                // Create a new thread for this client
                ClientHandler clientHandler = new ClientHandler(socket);

                Thread thread = new Thread(clientHandler);

                thread.start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // A separate handler for each client
    static class ClientHandler implements Runnable {

        private Socket socket;

        public ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {

            try (
                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter output = new PrintWriter(socket.getOutputStream(), true)
            ) {

                String message;


                // Continue communicating with this client
                while (true) {
                    // Read message from client
                    message = input.readLine();
                    // Client disconnected
                    if (message == null) {
                        break;
                    }

                    System.out.println("Client " + socket.getInetAddress() + " says: " + message);

                    // Check for client disconnect
                    if (message.equalsIgnoreCase("go down")) {
                        output.println("Goodbye! Disconnecting...");
                        break;
                    }
                    // Send response to client
                    output.println("Hello from the server! " + "You said: " + message);
                }

            } catch (IOException e) {
                System.out.println("Connection error with client: " + socket.getInetAddress());
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                System.out.println("Client disconnected: " + socket.getInetAddress());
            }
        }
    }
}
