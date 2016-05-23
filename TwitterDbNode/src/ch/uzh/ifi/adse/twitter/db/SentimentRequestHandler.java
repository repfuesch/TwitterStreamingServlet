package ch.uzh.ifi.adse.twitter.db;

import static com.mongodb.client.model.Filters.eq;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.bson.Document;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import ch.uzh.ifi.adse.twitter.analysis.*;
/*
 * Description:
 * FunctionHandler that takes a topic as input, stores the associated tweets in
 * the DB, and invokes for each List of results a Sentiment analysis handler
 * 
 */
public class SentimentRequestHandler implements RequestHandler<String, Double> {

    @Override
    public Double handleRequest(String topic, Context context) {
        context.getLogger().log("Input: " + topic);
        
        //Setup TweetManager. ResultSize determines #Tweets retrieved per iteration
		TweetManager tweetManager = new TweetManager(topic);
		tweetManager.setResultSize(10);
		tweetManager.InitQuery();
		
		//Todo: Store results in remote DB (either MonngoDB on EC2 instance or use Amazon DynamoDb)
		MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase("test");
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
		
		for(List<Tweet> tweets : tweetManager){
			tweets.stream().forEach(t -> {
				db.getCollection("tweets").insertOne(
						new Document()
								.append("id", new Long(t.getId()))
								.append("topic", t.getTopic())
								.append("createdAt", format.format(t.getCreatedAt()))
								.append("text", t.getMessage())
								.append("lang", t.getLanguage())
						);
			});
			
			//Todo: either call a function handler over amazon web api or use a db trigger (e.g. with dynamo-db)
			List<Long> ids = tweets.stream().map(t -> t.getId()).collect(Collectors.toList());
			System.out.println(ids);
			break;
		}
		
		mongoClient.close();
		
		//Todo: Aggregate and return sentiment analysis results from db
        return null;
    }

}
