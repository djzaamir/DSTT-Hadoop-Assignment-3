package CustomMapReduce;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CustomReduce extends Reducer {

    /*
    * Object Key      -> Is the unique key passed in to this reduce method
    *                    It will reduce the input values for this key only
    *
    * Iterable values -> List of values passed in for this reduce method
    *                    It will reduce these values, and put the result into the context against the input key
    *
    * Context context -> Is being used to communicate results back to the caller of this reduce method.
    * */
    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
