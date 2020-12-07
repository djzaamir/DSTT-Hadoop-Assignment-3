package CustomMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
* Responsible for bootstrapping the map reduce job, mapper and reducer modules
* It is also responsible for collecting the input data files
* And specifying the output directories for mapReduce results.
*
* */
public class MapReduceDriver extends Configured implements Tool {


    public MapReduceDriver(){}

    /*
     * Args contains the command line arguments, supplied by the user
     * */
    public MapReduceDriver(String args[]) throws Exception {
        ToolRunner.run(new MapReduceDriver(),args);
    }


    /*
    * Args contains the command line arguments, supplied by the user
    * */
    @Override
    public int run(String[] args) throws Exception {

        //Specifying custom XML tags for splitters, responsible for breaking up massive files into smaller chunks
        Configuration jobConf =  new Configuration();

        //Specify custom XML start and end tags, which will be used by Hadoop File Splitters
        jobConf.set("xmlinput.start", "<row ");
        jobConf.set("xmlinput.end", " />");

        //Creating a custom Hadoop job
        Job mapReduceJob = new Job(jobConf);


        //Specifying Main Hadoop Job Runner Jar class and its name
        mapReduceJob.setJarByClass(MapReduceDriver.class);
        mapReduceJob.setJobName("Map Reduce Job to calculate different metrics on apple.stackexchange data");

        //Specifying custom mapper and reducer classes for this job
        mapReduceJob.setMapperClass(CustomMap.class);
        mapReduceJob.setReducerClass(CustomReducePosts.class);


        //Specifying input and output paths
        FileInputFormat.addInputPath(mapReduceJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(mapReduceJob, new Path(args[1]));


        //Specifying the types of output key, value and its format
        mapReduceJob.setOutputKeyClass(Text.class);
        mapReduceJob.setOutputValueClass(LongWritable.class);
        mapReduceJob.setOutputFormatClass(TextOutputFormat.class);



        //Initiate job on hadoop
        int returnValue =  mapReduceJob.waitForCompletion(true) ? 0 : 1;

        if (mapReduceJob.isSuccessful())
            System.out.println("Successfully Completed MapReduce Job");
        else
            System.out.println("MapReduce job failed...");


        return returnValue;
    }
}
