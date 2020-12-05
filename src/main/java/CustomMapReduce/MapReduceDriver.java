package CustomMapReduce;

import org.apache.hadoop.conf.Configured;
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

    public MapReduceDriver(String args[]) throws Exception {
        ToolRunner.run(new MapReduceDriver(),args);
    }


    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }
}
