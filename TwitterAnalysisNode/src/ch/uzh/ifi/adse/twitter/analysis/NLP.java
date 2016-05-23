package ch.uzh.ifi.adse.twitter.analysis;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class NLP {
	
    private StanfordCoreNLP pipeline;

    public NLP(){
    	pipeline = new StanfordCoreNLP("MyPropFile.properties");
    }
    
    public int findSentiment(String tweetText) {

        int tweetSentiment = 0;
        if (tweetText != null && tweetText.length() > 0) {
            int longest = 0;
            Annotation annotation = pipeline.process(tweetText);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentAnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    tweetSentiment = sentiment;
                    longest = partText.length();
                }
            }
        }
        return tweetSentiment;
    }
}
