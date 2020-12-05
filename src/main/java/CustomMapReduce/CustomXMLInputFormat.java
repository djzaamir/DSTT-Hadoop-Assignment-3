package CustomMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CustomXMLInputFormat extends FileInputFormat {

    // These are just names of parameters that we will set in the bootstrapper file, while configuring a job
    // Then we can easily extract these parameters from the job configuration file
    public static final String  START_TAG = "xmlinput.start";
    public static final String END_TAG    = "xmlinput.end";


    //Responsible for returning an implementation for breaking down the file into chunks
    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
            return new XMLRecordReader();
    }

    //Our custom implementation for XML parsing
    private class XMLRecordReader extends RecordReader<LongWritable, Text> {

        // We will store the tags specified by the Job configuration from the bootstrapper file in these start and end tags
        // We will convert string into byte arrays, probably to speed things up, while trying to match tags from the disk
        // Byte I/O would be fasted instead of first converting them into strings
        private byte[] startTag;
        private byte[] endTag;

        // Start and End offset of the location, from where we are currently reading the data from
        private long start;
        private long end;

        // For Generating a File System stream on Hadoop DFS
        // We need hadoop specific file system manipulation classes
        // Because we are dealing with a distributed system, where data might reside on multiple nodes
        // THat is why regular filesystem manipulators can't be employed here
        private FSDataInputStream fsin;

        // For Creating and storing custom splits of XML
        private DataOutputBuffer buffer = new DataOutputBuffer();

        //Key and value
        private LongWritable key =  new LongWritable();
        private Text value =  new Text();

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {

            // Configuration will be passed from the job creation phase in the MapReduceDriver bootstrapper code
            // This conf contains many parameters along with the START and END XML tags, for which we are interested in
            Configuration conf =  taskAttemptContext.getConfiguration();

            // Extract start and end tags, and convert into bytes
            // We are converting them into bytes, because at the filesystem level, it is a lot easier to match a sequence of bytes
            // Instead of first converting them into Strings and then matching, which would add a lot more performance overlap
            startTag = conf.get(START_TAG).getBytes();
            endTag = conf.get(END_TAG).getBytes();

            /*
            * FileSplit is probably is a single input file, without doing any kind of splitting
            * Which has been loaded by hadoop
            * */
            FileSplit fileSplit = (FileSplit)inputSplit;


            //open the file and seek to the start of the split
            start = fileSplit.getStart();
            //Also calculate the end of file
            end   = start + fileSplit.getLength();

            //Get the path of the file, because we are about to open it ourself and parse it
            Path file  = fileSplit.getPath();

            //Get the link to input file, located on DFS
            FileSystem fs = file.getFileSystem(conf);


            // Because we won't be reading the complete file, and instead chunking it. We will be storing it in FSDataInputStream
            // It will be streaming data, instead of loading it all at once
            fsin = fs.open(fileSplit.getPath());

            //Goto the start of the file
            fsin.seek(start);

        }

        /*
        * Responsible for generating Key and Value from the input the data
        * chunking input data key and value pairs
        * */
        @Override
        public boolean nextKeyValue()
                throws IOException, InterruptedException {

            // If we still have data to read
            if (fsin.getPos() < end){

                //If we are able to find the starting tag
                if(readUntilMatch(startTag,false)){

                    //creating key and value pairs
                    try{

                        //Write the starting tag into buffer
                        buffer.write(startTag);

                        if (readUntilMatch(endTag, true)){
                            key.set(fsin.getPos());
                            value.set(buffer.getData(),0,buffer.getLength());

                            return true;
                        }
                    }finally {

                        //clear the output buffer which we are using for creating the `value`
                        buffer.reset();
                    }
                }
            }

            //No next value is available
            return false;
        }

        @Override
        public LongWritable getCurrentKey()
                throws IOException, InterruptedException {

            return key;
        }

        @Override
        public Text getCurrentValue()
                throws IOException, InterruptedException {

            return value;
        }

        @Override
        public float getProgress()
                throws IOException, InterruptedException {

            return (fsin.getPos() -  start) / (float)(end - start);
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

        /*
        * This method will be used to locate the starting tag and also ending tag
        *
        * If searching for starting tag, it only looks for the location of start_tag while moving the read pointer
        * It will keep moving the read pointer until it completly matches the starting tag
        *
        * */
        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
            int i  = 0;
            while (true){

                //Read data  byte and move to the next data byte
                int b = fsin.read();

                //end of file
                if (b == -1) return false;

                //save to buffer, if specified withinBlock, then it will also save data to the OutputBuffer
                if(withinBlock) buffer.write(b);

                //check if we're matching
                if (b == match[i]){
                    i++;
                    if (i >=  match.length) return true;
                }
                else i = 0;

                if (!withinBlock && i  == 0  && fsin.getPos() >= end)
                    return false;
            }
        }
    }
}
