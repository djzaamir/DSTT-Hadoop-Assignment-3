package CustomMapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;


/*
 * This class is responsible for parsing chunks of data assigned by hadoop.
 * Since input data is coming in XML, this class will first parse XML data.
 * After this it will extract different attributes of interest from each parsed XML record
 * And finally map it to a Key,Value pair as mapper's output
 * */
public class CustomMapPosts extends Mapper<LongWritable, Text, Text, Text> {

    /*
     * LongWritable Key, is the randomly generated key assigned by hadoop
     *
     * Text Value, is the input chunk assigned to this mapper
     * Context context, is the object which we are using to communicate with caller of this mapper, in order communicate
     *                   results back to the caller.
     * */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        try {

            //Responsible for parsing XML
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

            //In order to form a valid XML string with proper XML headers before sending it for parsing
            StringBuilder xmlStringBuilder = new StringBuilder();

            //Adding XML header tag
            xmlStringBuilder.append("<?xml version=\"1.0\"?>");

            //Creating XML string structure for the parser to parse
            xmlStringBuilder.append(value.toString());

            //Responsible for converting XML data into a byte array
            ByteArrayInputStream xmlInput = new ByteArrayInputStream(xmlStringBuilder.toString().getBytes("UTF-8"));

            //Initiate parse
            Document parsedDocument = documentBuilder.parse(xmlInput);


            //Extract node
            Node currentXMLRecord = parsedDocument.getDocumentElement();

            /*
             * CORE MAPPER CODE, RESPONSIBLE FOR GENERATING INPUT FOR REDUCERS

               From the parsed XML document, we are extracting different nodes of interest

               These nodes are  (Required by the assignment)
                1) Score
                2) CreationDate
                3) ViewCount

               Finally we are mapping Score and Viewcount as value and setting CreationDate year as the key

               To keep these two entities `Score` and `Viewcount` Separate` in the reducers
               We are prepending `Score` and `Viewcount` with different prefixes

               The purpose of doing this is to make sure that our reducers will receive different globally unique (In total record)
               but locally  (For individual reducer) same input datasets

               for example some reducers will receive data prepended with score, indicating it to crunch for scores
               others will receive data prepended with view, indicating it to crunch for `view`

               This will also result in separation of two types of data `score` and `view` in our final output

             * */

            //Extracting attributes map for this XML record
            NamedNodeMap attributesOfCurrentXMLRecord = currentXMLRecord.getAttributes();

            //Extracting nodes from a single xml record

            //Common attributes for both `Score` and `Viewcount`
            Node dateNode = attributesOfCurrentXMLRecord.getNamedItem("CreationDate"); //has to be present in every post
            Node scorePostBody  = attributesOfCurrentXMLRecord.getNamedItem("Body");
            Node scorePostTitle = attributesOfCurrentXMLRecord.getNamedItem("Title");

            Node scoreNode = attributesOfCurrentXMLRecord.getNamedItem("Score");

            Node viewCountNode = attributesOfCurrentXMLRecord.getNamedItem("ViewCount");


            Node UserId = attributesOfCurrentXMLRecord.getNamedItem("UserId");


            // Parsing date because, it is the common requirement for both `score` and `viewcount`, since this date will be used
            // as the key for both nodes will be date, with minor modifications
            String key_year = dateNode.getNodeValue().split("-")[0];


            //========================================Task-1 Specific Mapper Code ============================================

            // Extracting body and title into strings, to appended with score and view-counts
            String bodyStr   = "";
            String titleStr = "";

            //Only try to extract body and title if its not a User Post
            if (scorePostBody != null){
                bodyStr  = scorePostBody.getNodeValue();
            }
            if (scorePostTitle != null){
                titleStr = scorePostTitle.getNodeValue();
            }

            final String delimeterToSegregateData = "<DELIMETER_TAG>";
            //These data nodes might by empty need some error checking
            //Make sure for valid scoreNode
            if (scoreNode != null) {
                String score = String.valueOf(scoreNode.getNodeValue());


                //Creating hadoop keys and values
                Text hadoop_value = new Text(score  + delimeterToSegregateData + titleStr + delimeterToSegregateData + bodyStr);

                //prefix for score
                String score_key_identifier = "score";

                //Creating key with a prefix, to segregate data in reducers
                String final_score_key = score_key_identifier + "<>" + key_year;
                Text hadoop_key = new Text(final_score_key);

                //Write to hadoop's mapper output
                context.write(hadoop_key, hadoop_value);
            }

            //Making sure for valid data of viewCount
            if (viewCountNode != null) {
                String viewCount = String.valueOf(viewCountNode.getNodeValue());

                //Creating hadoop keys and values
                Text hadoop_value = new Text(viewCount + delimeterToSegregateData + titleStr + delimeterToSegregateData + bodyStr);

                //prefix for viewcount
                String viewcount_key_identifier = "view";

                //Creating key with a prefix, to segregate data in reducers
                String final_view_key = viewcount_key_identifier + "<>" + key_year;
                Text hadoop_key = new Text(final_view_key);

                //Write to hadoop's mapper output
                context.write(hadoop_key, hadoop_value);
            }



          //========================================Task-2 Specific Mapper Code ============================================


            //Making sure for valid data for UserId
            if (UserId != null) {
                String userId = String.valueOf(UserId.getNodeValue());

                //Skip UserId == 1, as indicated by the assignment
                if (Long.parseLong(userId) != 1l){
                    //Creating hadoop keys and values
                    Text hadoop_value = new Text(userId + delimeterToSegregateData);

                    //prefix for viewcount
                    String viewcount_key_identifier = "user_id";

                    //Creating key with a prefix, to segregate data in reducers
                    String final_view_key = viewcount_key_identifier + "<>" + key_year;
                    Text hadoop_key = new Text(final_view_key);

                    //Write to hadoop's mapper output
                    context.write(hadoop_key, hadoop_value);
                }
            }

        } catch (ParserConfigurationException | SAXException e) {
            e.printStackTrace();
        }
    }

}
