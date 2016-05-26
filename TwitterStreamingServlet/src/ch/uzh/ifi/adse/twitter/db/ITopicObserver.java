package ch.uzh.ifi.adse.twitter.db;

import java.util.List;
/**
 * Interface to update an Object when topics have changed
 * @author Florian Schüpfer
 *
 */
public interface ITopicObserver {
	void TopicsChanged(List<String> topics);
}
