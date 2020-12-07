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
* Todo Add Explanation about the working of this map class
* */
public class CustomMapPosts extends Mapper<LongWritable, Text, Text, LongWritable> {

    /*
    * TODO what is LongWritable Key
    *
    * Text Value, is the input chunk assigned to this mapper
    * Context context, is the object which we are using to communicate with caller of this mapper, in order communicate
    *                   results back to the caller.
    * */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //Parsing XML `value`, because the the splitters might have assigned big chunks of XML strings to individual mappers

        try {
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

            StringBuilder xmlStringBuilder = new StringBuilder();

            //Adding XML header tag
            xmlStringBuilder.append("<?xml version=\"1.0\"?>");

            //Creating XML string structure for the parser to parse
            xmlStringBuilder.append("<ROOT-TAG>");
            xmlStringBuilder.append(value.toString());
            xmlStringBuilder.append("</ROOT-TAG>");

            ByteArrayInputStream xmlInput =  new ByteArrayInputStream(xmlStringBuilder.toString().getBytes("UTF-8"));

            //Initiate parse
            Document parsedDocument =  documentBuilder.parse(xmlInput);

            Element rootNode =  parsedDocument.getDocumentElement();

            NodeList InnerXMLRecordsList = rootNode.getElementsByTagName("row");

            for (int i = 0 ; i < InnerXMLRecordsList.getLength(); i++){
                Node currentXMLRecord =  InnerXMLRecordsList.item(i);

                /*
                 * CORE MAPPER CODE, RESPONSIBLE FOR GENERATING INPUT FOR REDUCERS
                 * TODO Please explain the working of this mapper function
                 * */
                NamedNodeMap attributesOfCurrentXMLRecord = currentXMLRecord.getAttributes();

                Node scoreNode     = attributesOfCurrentXMLRecord.getNamedItem("Score");
                Node dateNode      = attributesOfCurrentXMLRecord.getNamedItem("CreationDate"); //has to be present in every post
                Node viewCountNode = attributesOfCurrentXMLRecord.getNamedItem("ViewCount");


                // Parsing date because, it is the common requirement for both score and viewcount, since this date will be used
                // as the key for both nodes will be date, with minor modifications
                String key_year =  dateNode.getNodeValue().split("-")[0];


                //These data nodes might by empty need some error checking
                if (scoreNode != null){
                    Long score =  Long.parseLong(scoreNode.getNodeValue());


                    //Creating hadoop keys and values
                    LongWritable hadoop_value = new LongWritable(score);

                    String score_key_identifier = "score";
                    String final_score_key      = score_key_identifier  + "<>" +  key_year;
                    Text hadoop_key = new Text(final_score_key);

                    //Write to hadoop's mapper output
                    context.write(hadoop_key, hadoop_value);
                }

                if(viewCountNode != null){
                    Long viewCount = Long.parseLong(viewCountNode.getNodeValue());

                    //Creating hadoop keys and values
                    LongWritable hadoop_value = new LongWritable(viewCount);

                    String viewcount_key_identifier = "view";
                    String final_view_key          = viewcount_key_identifier + "<>" +  key_year;
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
