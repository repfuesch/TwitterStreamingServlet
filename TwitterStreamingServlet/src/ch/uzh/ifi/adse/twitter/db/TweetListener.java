package ch.uzh.ifi.adse.twitter.db;

import twitter4j.RateLimitStatusEvent;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class TweetListener implements StatusListener, twitter4j.util.function.Consumer<RateLimitStatusEvent>, AutoCloseable {
	
	private DynamoDB dynamoDB;
	private String topic;
	
	public TweetListener(String topic){
		AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
		client.withEndpoint("http://localhost:8000");
		dynamoDB = new DynamoDB(client);
		this.topic = topic;
	}
	
	@Override
	public void onException(Exception arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onStatus(Status tweet) {
		
		Table table = dynamoDB.getTable("Tweets");
		
		Item item = new Item()     
			    .withPrimaryKey("Id", tweet.getId())
			    .withString("text", tweet.getText())
			    .withLong("createdAt", tweet.getCreatedAt().getTime())
			    .withString("topic", topic);
		
		try{
			PutItemOutcome outcome = table.putItem(item);
		}catch(AmazonServiceException ex)
		{
			System.out.println("Exception: " + ex.getMessage());
		}
		
		String bla = "bla";
	}
	
	private void CleanTable()
	{
		Map<String, AttributeValue> expressionAttributeValues = 
			    new HashMap<String, AttributeValue>();
		expressionAttributeValues.put(":topic", new AttributeValue().withS(topic));
		ScanRequest scanRequest = new ScanRequest()
			    .withTableName("Tweets")
			    .withFilterExpression("topic = :topic")
			    .withProjectionExpression("Id")
			    .withExpressionAttributeValues(expressionAttributeValues);


		ScanResult result = ((AmazonDynamoDB) dynamoDB).scan(scanRequest);
		
		Table table = dynamoDB.getTable("Tweets");
		result.getItems().forEach(item -> table.deleteItem("Id", item.get("Id")));
	}

	@Override
	public void onTrackLimitationNotice(int arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onScrubGeo(long arg0, long arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStallWarning(StallWarning arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void accept(RateLimitStatusEvent t) 
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws Exception {
		CleanTable();
	}

}
