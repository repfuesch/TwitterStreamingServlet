package ch.uzh.ifi.adse.twitter.db;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;

/**
 * Class for managing tweets and topics in the database. Provides functionality for insertion and deletion of tweets/topics.
 * Informs subscribers when topics on the database change.
 * @author Florian Schüpfer
 *
 */
public class TweetManager {
	
	private DynamoDB dynamoDB;
	private List<String> topics;
	private List<ITopicObserver> observers;
	
	public TweetManager()
	{
		AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ClasspathPropertiesFileCredentialsProvider("aws_credentials.properties"));
		client.setRegion(Region.getRegion(Regions.US_EAST_1));
		dynamoDB = new DynamoDB(client);
		this.topics = new ArrayList<>();
		this.observers = new ArrayList<>();
	}
	
	public final List<String> GetTopics()
	{
		return topics;
	}
	
	/**
	 * Adds an observer
	 */
	public void AddObserver(ITopicObserver observer)
	{
		if(!observers.contains(observer))
			observers.add(observer);
	}
	
	/**
	 * removes an observer
	 */
	public void RemoveObserver(ITopicObserver observer)
	{
		if(observers.contains(observer))
			observers.remove(observer);
	}
	
	/**
	 * Inserts a topic into the database
	 */
	public void InsertTopic(String topic)
	{
		Table table = dynamoDB.getTable("topics");
		Item item = new Item()     
			    .withPrimaryKey("id", topic)
			    .with("topic", topic);
		
		table.putItem(item);
		UpdateTopics();
	}
	
	/**
	 * Removes a topic from the database
	 */
	public void RemoveTopic(String topic)
	{
		Table table = dynamoDB.getTable("topics");
        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
        .withPrimaryKey("id", topic);
    	table.deleteItem(deleteItemSpec);
    	UpdateTopics();
	}
	
	/**
	 * Updates topics from the database
	 */
	public void UpdateTopics()
	{
		List<String> dbTopics = new ArrayList<>();
		Table table = dynamoDB.getTable("topics");
        ItemCollection<ScanOutcome> items = table.scan(null, null, null, null);
        
        items.iterator().forEachRemaining((i) -> {
        	dbTopics.add(i.getString("topic"));
        });
        
        if(!(topics.containsAll(dbTopics) && topics.size() == dbTopics.size())){
        	topics = dbTopics;
        	observers.forEach(o -> o.TopicsChanged(topics));
        }
	}
	
	/**
	 * Inserts into the tweets collection in the DB
	 */
	public void InsertTweet(long id, long createdAt, String lang, String text, String topic)
	{
		Table table = dynamoDB.getTable("tweets");
		
		Item item = new Item()     
			    .withPrimaryKey("id", id)
			    .with("created_at", createdAt)
			    .with("lang", lang)
			    .withString("text", text)
			    .withString("topic", topic);
		
		PutItemOutcome outcome = table.putItem(item);
	}

	/**
	 * Deletes all items from the table 'TweetCollection'
	 */
	public void DeleteTweets()
	{
		Table table = dynamoDB.getTable("tweets");
        ItemCollection<ScanOutcome> items = table.scan(null, "id", null, null);
        
        items.iterator().forEachRemaining((i) -> {
            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
            .withPrimaryKey("id", i.get("id"));
        	table.deleteItem(deleteItemSpec);
        });
        UpdateTopics();
	}
}
