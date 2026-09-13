package edu.utexas.cs.cs378;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RMIServer {

    static public int portNumber = 33333;

    public static void main(String[] args) {

        if (args.length > 0) {
            System.err.println("Usage: RMIServer <port number> ");
            portNumber = Integer.parseInt(args[0]);
        }

        try {
            // Create the remote object
            DataServiceImpl service = new DataServiceImpl();

            // Start RMI Registry
            Registry registry = LocateRegistry.createRegistry(portNumber);

            // Register the remote object
            registry.rebind("DataService", service);

            System.out.println("RMI Server is running...");
            System.out.println("Waiting for clients...");


            // Keep the server running
            synchronized (RMIServer.class) {
                RMIServer.class.wait();
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

