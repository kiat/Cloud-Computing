package edu.utexas.cs.cs378;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataService extends Remote {

    // Client sends an array of integers.
    // Server computes statistics and returns a String.
    String analyzeData(int[] data) throws RemoteException;
}

