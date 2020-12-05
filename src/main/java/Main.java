/*
* Main Driver Class
* Responsible for creating hadoop jobs, submitting them to hadoop cluster and collecting results
* */

import CustomMapReduce.*;

public class Main {

    private static MapReduceDriver mapReduceDriver;

    public static void main(String[] args) throws Exception {

        System.out.println("Initiating MapReduce...");

        mapReduceDriver =  new MapReduceDriver(args);
    }
}
