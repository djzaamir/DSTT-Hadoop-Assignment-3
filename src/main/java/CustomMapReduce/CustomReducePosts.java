package CustomMapReduce;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CustomReducePosts extends Reducer {

    private final String delimeterToSegregateData = "<DELIMETER_TAG>";
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
        String highest_value_post = "";
        String highest_value_title = "";

        Iterator valuesIterable = values.iterator();
        while (valuesIterable.hasNext()){

            String splittedData[] = valuesIterable.next().toString().split(delimeterToSegregateData);

            long  current_numeric_value = Long.parseLong(splittedData[0]);


             if(current_numeric_value > highest_value){
                 highest_value       = current_numeric_value;

                 if (splittedData.length > 1){
                     highest_value_post  = splittedData[1];
                     highest_value_title = splittedData[2];
                 }
             }

        }

        //write to the context for further hadoop processing
        context.write(key, highest_value + delimeterToSegregateData +
                           highest_value_title + delimeterToSegregateData + highest_value_post);
    }
}
