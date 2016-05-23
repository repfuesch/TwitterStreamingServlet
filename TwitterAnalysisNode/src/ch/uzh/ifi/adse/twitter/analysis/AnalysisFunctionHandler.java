package ch.uzh.ifi.adse.twitter.analysis;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;

/*
 * Description:
 * FunctionHandler that takes a list of ID's as input, retrieves the associated tweets in
 * the DB, performs a sentiment analysis for each tweet and stores the results in a result table in
 * the same DB 
 */
public class AnalysisFunctionHandler implements RequestHandler<List<Long>, List<Integer>> {

	/*
	 * @input: Array of object IDs from the object store
	 * @output: Array of sentiments (positive integer) for each tweet
	 */
    @Override
    public List<Integer> handleRequest(List<Long> tweetIds, Context context) {
        context.getLogger().log("Input: " + tweetIds);

        NLP processor = new NLP();
        
		//Todo: Retrieve and store results from remote DB (either MonngoDB on EC2 instance or use Amazon DynamoDb)
		MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase("test");
		MongoCollection<Document> inputCollection = db.getCollection("tweets");
		MongoCollection<Document> outputCollection = db.getCollection("tweetResults");

		List<Integer> sentimentList = new ArrayList<>();
		for(Long id : tweetIds){
			Document result = inputCollection.find(eq("id", id)).first();
			String text = result.getString("text");
			int sentiment = processor.findSentiment(text);
			sentimentList.add(sentiment);
			outputCollection.insertOne(new Document(id.toString(), sentiment));
		}
		
		mongoClient.close();
		
        return sentimentList;
    }

}
