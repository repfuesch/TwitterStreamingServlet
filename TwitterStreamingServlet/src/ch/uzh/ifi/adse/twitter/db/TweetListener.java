package ch.uzh.ifi.adse.twitter.db;

import twitter4j.RateLimitStatusEvent;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;

public class TweetListener implements StatusListener, twitter4j.util.function.Consumer<RateLimitStatusEvent> {
	
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

}
