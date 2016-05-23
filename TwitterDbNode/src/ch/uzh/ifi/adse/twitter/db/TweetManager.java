package ch.uzh.ifi.adse.twitter.db;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class TweetManager implements Iterator<List<Tweet>>, Iterable<List<Tweet>> {
	
	private Twitter twitter;
	private int resultSize;
	private QueryResult queryResult;
	private Query query;
	private String topic;
	
	public TweetManager(String topic){
		twitter = new TwitterFactory().getInstance();
		setResultSize(20);
		this.topic = topic;
	}
	
	public void InitQuery(){
	       query = new Query(topic);
	       query.setCount(resultSize);
	       
	       try{
	    	   queryResult = twitter.search(query);  
	       }catch (TwitterException te) {
		       te.printStackTrace();
		   }
	}
	
	public List<Tweet> getNextTweets() {
	
	   List<Tweet> tweetList = new ArrayList<Tweet>();
	   try {
           queryResult = twitter.search(query);
           List<Status> tweets = queryResult.getTweets();
           for (Status status : tweets) {
        	   
        	   Tweet tweet = new Tweet();
        	   tweet.setCreatedAt(status.getCreatedAt());
        	   tweet.setId(status.getId());
        	   tweet.setLanguage(status.getLang());
        	   tweet.setTopic(topic);
        	   tweet.setMessage(status.getText());
        	   tweetList.add(tweet);
           }
	       
	   } catch (TwitterException te) {
	       te.printStackTrace();
	       System.out.println("Failed to search tweets: " + te.getMessage());
	   }
	   
	   return tweetList;
	}

	public int getResultSize() {
		return resultSize;
	}

	public void setResultSize(int resultSize) {
		this.resultSize = resultSize;
	}

	@Override
	public boolean hasNext() {
		return queryResult.hasNext();
	}

	@Override
	public List<Tweet> next() {
		return getNextTweets();
	}

	@Override
	public Iterator<List<Tweet>> iterator() {
		return this;
	}

}
