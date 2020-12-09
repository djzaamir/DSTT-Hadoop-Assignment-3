/*
* Main Driver Class
* Responsible for creating hadoop jobs, submitting them to hadoop cluster and collecting results
* */

import CustomMapReduce.*;

public class Main {

    private static MapReduceDriver mapReduceDriver;

    public static void main(String[] args) throws Exception {

        System.out.println("Initiating MapReduce...");

        // TODO, Some basic error checking, like do the input and output paths even exist
        if(args.length < 2){
            System.out.println("Please Specify Input Directory, and Output directories");
            return;
        }


            // Starting bootstrapping driver class, which is responsible for initiating MapReduce tasks
            // We are also passing in the command line arguments into the driver class, because they contain Input dir and Output dir
            // The constructor method of this class will automatically begin the bootstrapping process
            mapReduceDriver =  new MapReduceDriver(args);
    }
}
