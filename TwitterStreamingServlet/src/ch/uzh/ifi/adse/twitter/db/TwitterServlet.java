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
	
	private TwitterStream stream;
	private TweetListener listener;
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public TwitterServlet() {
        super();
    }
    
    private void RegisterTopic(String topic)
    {
    	if(stream == null)
    	{
            stream = new TwitterStreamFactory().getInstance();
            listener = new TweetListener();
            stream.onRateLimitReached(listener);
            stream.addListener(listener);
    	}

    	listener.UpdateTopics("#"+ topic);
        FilterQuery fq = new FilterQuery();
        String[] topics = new String[listener.GetTopics().size()];
        String keywords[] = listener.GetTopics().toArray(topics);
        fq.track(keywords);
        stream.filter(fq);
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
		
		if(listener != null && listener.GetTopics().contains(topic))
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
		
		if(!listener.GetTopics().contains(topic))
		{
			response.setStatus(403);
			response.getWriter().append("Topic is not tracked!");
			return;
		}
		
    	listener.RemoveTopic(topic);
		response.getWriter().append("Topic removed");
	}
}
