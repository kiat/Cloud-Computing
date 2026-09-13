package edu.utexas.cs.cs378;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;


public class DataServiceImpl extends UnicastRemoteObject implements DataService {

    public DataServiceImpl() throws RemoteException {
        super();
    }

    @Override
    public String analyzeData(int[] data) throws RemoteException {

        System.out.println("Received dataset from client.");

        if (data == null || data.length == 0) {
            return "Dataset is empty.";
        }

        int sum = 0;
        int min = data[0];
        int max = data[0];

        for (int value : data) {
            sum += value;

            if (value < min) {
                min = value;
            }

            if (value > max) {
                max = value;
            }
        }

        double average = (double) sum / data.length;

        return "Count = " + data.length +
                ", Sum = " + sum +
                ", Average = " + average +
                ", Min = " + min +
                ", Max = " + max;
    }
}