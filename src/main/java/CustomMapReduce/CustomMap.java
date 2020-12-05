package CustomMapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/*
* Todo Add Explanation about the working of this map class
* */
public class CustomMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    /*
    * TODO what is LongWritable Key
    *
    * Text Value, is the input chunk assigned to this mapper
    * Context context, is the object which we are using to communicate with caller of this mapper, in order communicate
    *                   results back to the caller.
    * */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
