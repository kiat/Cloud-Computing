package edu.utexas.cs.cs378;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.util.Arrays;
import java.util.Scanner;

public class RMIClient {

    static public int portNumber = 33333;
    static public String serverAddress = "localhost";

    public static void main(String[] args) {

        if (args.length >= 1) {
            System.err.println("Usage: RMIClient <hostname> <port number> ");
            serverAddress = args[0];
            portNumber = Integer.parseInt(args[1]);
        }


        try {
            // Connect to RMI Registry
            Registry registry = LocateRegistry.getRegistry(serverAddress, portNumber);

            // Find the remote object
            DataService service = (DataService) registry.lookup("DataService");


            // Dataset created by the client
            int[] data = {10, 20, 30, 40, 50};
            System.out.println("Sending dataset: " + Arrays.toString(data));

            // Call remote method
            String result = service.analyzeData(data);

            // Display result returned by server
            System.out.println("Result from server:");
            System.out.println(result);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}