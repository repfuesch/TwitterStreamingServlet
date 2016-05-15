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
 * Servlet implementation class TwitterServlet
 */
@WebServlet("/tweets")
public class TwitterServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	private Map<String, TwitterStream> streams;
	private Map<TwitterStream, TweetListener> listeners;
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public TwitterServlet() {
        super();
        streams = new HashMap<>();
        listeners = new HashMap<>();
    }
    
    private void RegisterTopic(String topic)
    {
        TwitterStream stream = new TwitterStreamFactory().getInstance();

        FilterQuery fq = new FilterQuery();
        String keywords[] = { topic };
        fq.track(keywords);
        
        TweetListener tweetListener = new TweetListener(topic);
        stream.onRateLimitReached(tweetListener);
        stream.addListener(tweetListener);
        stream.filter(fq);
        
        listeners.put(stream, tweetListener);
        streams.put(topic, stream);
    }
    
    private void UnregisterTopic(String topic)
    {
    	TwitterStream stream = streams.get(topic);
    	
    	try {
			listeners.get(stream).close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
    	streams.get(topic).cleanUp();
    	listeners.remove(stream);
    	streams.remove(topic);
    }
    
    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String topic = request.getParameter("topic");
		
		if(topic == null)
		{
			response.setStatus(403);
			response.getWriter().append("Parameter 'topic' must be set!");
			return;
		}
		
		if(streams.containsKey(topic))
		{
			response.setStatus(403);
			response.getWriter().append("Topic already tracked!");
			return;
		}
		
		RegisterTopic(topic);
		response.getWriter().append("Topic tracked");
	}
	
	protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String topic = request.getParameter("topic");
		
		if(topic == null)
		{
			response.setStatus(403);
			response.getWriter().append("Parameter 'topic' must be set!");
			return;
		}
		
		if(!streams.containsKey(topic))
		{
			response.setStatus(403);
			response.getWriter().append("Topic is not tracked!");
			return;
		}
		
		UnregisterTopic(topic);
		response.getWriter().append("Topic untracked");
	}
}
