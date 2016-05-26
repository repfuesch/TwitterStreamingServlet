package ch.uzh.ifi.adse.twitter.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import twitter4j.*;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * Servlet implementation: Provides Methods to add or remove topics from the TwitterStream
 * @author Florian Schüpfer
 */
@WebServlet("/topics")
public class TwitterServlet extends HttpServlet implements ITopicObserver{
	private static final long serialVersionUID = 1L;
	
	private TwitterStream stream;
	private TweetListener listener;
	private TweetManager tweetManager;
	
    public TwitterServlet() {
        super();
        
        stream = new TwitterStreamFactory().getInstance();
        tweetManager = new TweetManager();
        tweetManager.AddObserver(this);
        listener = new TweetListener(tweetManager);
        stream.addListener(listener);
    }
    
    /**
     * Adds a topic to the TweetManager
     */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		SetHeaders(response);
		String topic = request.getParameter("topic");
		
		if(topic == null)
		{
			response.setStatus(403);
			response.getWriter().append("\nParameter 'topic' must be set!\n");
			return;
		}
		
		if(tweetManager.GetTopics().contains(topic))
		{
			response.setStatus(403);
			response.getWriter().append("\nTopic already tracked!\n");
			return;
		}
		
    	tweetManager.InsertTopic("#"+ topic);
		response.getWriter().append("\nTopic tracked\n");
	}
	
	/**
	 * Deletes a topic from the tweetManager or stops streaming entirely when no topic is provided
	 */
	protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		SetHeaders(response);
		String topic = request.getParameter("topic");
		
		if(topic == null)
		{
			tweetManager.DeleteTweets();
			stream.cleanUp();
			listener = null;
			stream = null;
			response.getWriter().append("\nStreaming stopped. All pending tweets deleted.\n");
			
		}else{
			tweetManager.RemoveTopic(topic);
			response.getWriter().append("\nTopic removed\n");
		}
	}
	
	private void SetHeaders(HttpServletResponse response)
	{
        response.setContentType("application/json");            
        response.setCharacterEncoding("UTF-8");
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "POST,DELETE");
        response.setHeader("Access-Control-Allow-Headers", "Content-Type");
        response.setHeader("Access-Control-Max-Age", "86400");
	}

	@Override
	public void TopicsChanged(List<String> topics) {
        FilterQuery fq = new FilterQuery();
        String[] topicArray = new String[topics.size()];
        String keywords[] = topics.toArray(topicArray);
        fq.track(keywords);
        stream.filter(fq);
	}
	
}
