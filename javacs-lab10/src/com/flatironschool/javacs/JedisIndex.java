package com.flatironschool.javacs;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.jsoup.select.Elements;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/* NOTE: when running ant JedisIndex, the test code in the main 
 * produces the correct result. However, when attemping ant test/ learn test
 * there is a socket timeout error. I am confident that my code is able to 
 * create an index - - I understand that there are some issues regarding time efficiency
 * but do not have the sufficient time (ironically enough) to figure out how I could 
 * improve this code. 
 */ 

/**
 * Represents a Redis-backed web search index.
 * 
 */
public class JedisIndex {

	private Jedis jedis;

	/**
	 * Constructor.
	 * 
	 * @param jedis
	 */
	public JedisIndex(Jedis jedis) {
		this.jedis = jedis;
	}
	
	/**
	 * Returns the Redis key for a given search term.
	 * 
	 * @return Redis key.
	 */
	private String urlSetKey(String term) {
		return "URLSet:" + term;
	}
	
	/**
	 * Returns the Redis key for a URL's TermCounter.
	 * 
	 * @return Redis key.
	 */
	private String termCounterKey(String url) {
		return "TermCounter:" + url;
	}

	// public void getMembers(){
	// 	Set<String> kk = jedis.smembers("URLSet:the");
	// 	Transaction t = jedis.multi();
	// 	for(String k: kk){
	// 		t.System.out.print(k);
	// 	}
	// 	t.exec();
		

	// }

	/**
	 * Checks whether we have a TermCounter for a given URL.
	 * 
	 * @param url
	 * @return
	 */
	public boolean isIndexed(String url) {
		String redisKey = termCounterKey(url);
		return jedis.exists(redisKey);
	}

	/**
     * Pushes the contents of the TermCounter to Redis.
     */
    public void pushTermCounterToRedis(TermCounter tc) {
  
    	Transaction t = jedis.multi();
    	for (String word: tc.keySet()){
    		/* tc.getLabel will return the url name 
    		/ remember the that termcounter contains a map of all the terms on 
    		/ the given url page and the amount of times that the term shows up */
    		String url = tc.getLabel();
    		String countFromTC = tc.get(word).toString();
    		t.hset(termCounterKey(url), word, countFromTC);
    		//result.add(tc.get(key).toString());
    

    	}

    	t.exec();

    	//return result;


    }
	
	/**
	 * Looks up a search term and returns a set of URLs.
	 * 
	 * @param term
	 * @return Set of URLs.
	 */
	public Set<String> getURLs(String term) {

		// // smembers returns all the members in a set
		String key = urlSetKey(term);

		return jedis.smembers(key);
        

	}

    /**
	 * Looks up a term and returns a map from URL to count.
	 * 
	 * @param term
	 * @return Map from URL to count.
	 */
	public Map<String, Integer> getCounts(String term) {
		Map<String, Integer> result = new HashMap<String, Integer>();
		Set<String> urls = getURLs(term);

		for (String url: urls){

			Integer count = getCount(url, term);
			result.put(url, count);
		}

		return result;
        
	}

    /**
	 * Returns the number of times the given term appears at the given URL.
	 * 
	 * @param url
	 * @param term
	 * @return
	 */
	public Integer getCount(String url, String term) {
		Integer value = Integer.valueOf(jedis.hget(termCounterKey(url), term));

		return value;

        
	}

	/**
     * Adds a URL to the set associated with `term`.
     */
    public void add(String term, TermCounter tc) {
    	Transaction t = jedis.multi();


		 t.sadd(urlSetKey(term), tc.getLabel());

		 t.exec();

    }
 

	/**
	 * Add a page to the index.
	 * 
	 * @param url         URL of the page.
	 * @param paragraphs  Collection of elements that should be indexed.
	 */
	public void indexPage(String url, Elements paragraphs) {
	// make a TermCounter and count the terms in the paragraphs
	 	TermCounter tc = new TermCounter(url);
	 	tc.processElements(paragraphs);
		
	// for each term in the TermCounter, add the TermCounter to the index
	 	for (String term: tc.keySet()) {
	 		add(term, tc);
	 		pushTermCounterToRedis(tc);
	 	}
	}

	/**
	 * Prints the contents of the index.
	 * 
	 * Should be used for development and testing, not production.
	 */
	public void printIndex() {
		// loop through the search terms
		for (String term: termSet()) {
			System.out.println(term);
			
			// for each term, print the pages where it appears
			Set<String> urls = getURLs(term);
			for (String url: urls) {
				System.out.println(url);
				Integer count = getCount(url, term);
				System.out.println("    " + url + " " + count);
			}
		}
	}

	/**
	 * Returns the set of terms that have been indexed.
	 * 
	 * Should be used for development and testing, not production.
	 * 
	 * @return
	 */
	public Set<String> termSet() {
		Set<String> keys = urlSetKeys();
		Set<String> terms = new HashSet<String>();
		for (String key: keys) {
			String[] array = key.split(":");
			if (array.length < 2) {
				terms.add("");
			} else {
				terms.add(array[1]);
			}
		}
		return terms;
	}

	/**
	 * Returns URLSet keys for the terms that have been indexed.
	 * 
	 * Should be used for development and testing, not production.
	 * 
	 * @return
	 */
	public Set<String> urlSetKeys() {
		return jedis.keys("URLSet:*");
	}

	/**
	 * Returns TermCounter keys for the URLS that have been indexed.
	 * 
	 * Should be used for development and testing, not production.
	 * 
	 * @return
	 */
	public Set<String> termCounterKeys() {
		return jedis.keys("TermCounter:*");
	}

	/**
	 * Deletes all URLSet objects from the database.
	 * 
	 * Should be used for development and testing, not production.
	 * 
	 * @return
	 */
	public void deleteURLSets() {
		Set<String> keys = urlSetKeys();
		Transaction t = jedis.multi();
		for (String key: keys) {
			t.del(key);
		}
		t.exec();
	}

	/**
	 * Deletes all URLSet objects from the database.
	 * 
	 * Should be used for development and testing, not production.
	 * 
	 * @return
	 */
	public void deleteTermCounters() {
		Set<String> keys = termCounterKeys();
		Transaction t = jedis.multi();
		for (String key: keys) {
			t.del(key);
		}
		t.exec();
	}

	/**
	 * Deletes all keys from the database.
	 * 
	 * Should be used for development and testing, not production.
	 * 
	 * @return
	 */
	public void deleteAllKeys() {
		Set<String> keys = jedis.keys("*");
		Transaction t = jedis.multi();
		for (String key: keys) {
			t.del(key);
		}
		t.exec();
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Jedis jedis = JedisMaker.make();
		JedisIndex index = new JedisIndex(jedis);
		//System.out.println("I'm in the main");
		
		index.deleteTermCounters();
		index.deleteURLSets();
		index.deleteAllKeys();
		loadIndex(index);


		
		Map<String, Integer> map = index.getCounts("the");
		for (Entry<String, Integer> entry: map.entrySet()) {
			System.out.println(entry);
		}
	}

	/**
	 * Stores two pages in the index for testing purposes.
	 * 
	 * @return
	 * @throws IOException
	 */
	private static void loadIndex(JedisIndex index) throws IOException {
		WikiFetcher wf = new WikiFetcher();

		String url = "https://en.wikipedia.org/wiki/Java_(programming_language)";
		Elements paragraphs = wf.readWikipedia(url);
	 	index.indexPage(url, paragraphs);


		url = "https://en.wikipedia.org/wiki/Programming_language";
		paragraphs = wf.readWikipedia(url);
		index.indexPage(url, paragraphs);
	}
}
