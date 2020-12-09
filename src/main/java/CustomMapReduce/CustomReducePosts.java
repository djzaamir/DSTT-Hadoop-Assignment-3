package CustomMapReduce;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CustomReducePosts extends Reducer {

    /*
    * Object Key      -> Is the unique key passed in to this reduce method
    *                    It will reduce the input values for this key only
    *
    * Iterable values -> List of values passed in for this reduce method
    *                    It will reduce these values, and put the result into the context against the input key
    *
    * Context context -> Is being used to communicate results back to the caller of this reduce method.
    *
    * Important, since all values provided to this reducer will have the same key, hadoop will make sure to push same set of keys
    * for each reducer function, this allows us to crunch numbers against particular keys, very easily
    * */
    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        Long highest_value = 0l;

        Iterator valuesIterable = values.iterator();
        while (valuesIterable.hasNext()){
             long  current_value = Long.parseLong(valuesIterable.next().toString());
             if(current_value > highest_value)
                 highest_value = current_value;
        }

        //write to the context for further hadoop processing
        context.write(key, highest_value);
    }
}
