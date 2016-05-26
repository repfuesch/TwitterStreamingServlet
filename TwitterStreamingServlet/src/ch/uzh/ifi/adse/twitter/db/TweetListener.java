package ch.uzh.ifi.adse.twitter.db;

import twitter4j.HashtagEntity;
import twitter4j.RateLimitStatusEvent;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
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
/**
 * Listens to status updates from Twitter for the registered topics.
 * Author: Florian Schüpfer
 * Date: 26.05.2016
 */
public class TweetListener implements StatusListener {
	
	private TweetManager tweetManager;
	
	public TweetListener(TweetManager tweetManager){
		
		this.tweetManager = tweetManager;
	}

	@Override
	public void onException(Exception arg0) {
	}

	@Override
	public void onStatus(Status tweet) {
		
		tweetManager.UpdateTopics();
		HashtagEntity[] hashtags = tweet.getHashtagEntities();
		String referenced_topic = null;
		for(HashtagEntity entity : hashtags)
		{
			for(String topic : tweetManager.GetTopics())
			{
				if(("#" + entity.getText()).equalsIgnoreCase(topic))
				{
					referenced_topic = topic;
					break;
				}
				if(referenced_topic != null)
					break;
			}
		}
		
		tweetManager.InsertTweet(tweet.getId(), tweet.getCreatedAt().getTime(), tweet.getLang(), tweet.getText(), referenced_topic);
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
}
