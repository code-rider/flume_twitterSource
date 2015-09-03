package xulu;


import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;


public class GeoLocationSource extends AbstractSource
implements EventDrivenSource, Configurable {
  private static final Logger logger =
      LoggerFactory.getLogger(TwitterSource.class);
  
  /** Information necessary for accessing the Twitter API */
  private String consumerKey;
  private String consumerSecret;
  private String accessToken;
  private String accessTokenSecret;
  
  private String[] keywords;
  private double[][] location;
  /** The actual Twitter stream. It's set up to collect raw JSON data */
  private  TwitterStream twitterStream;

  /**
   * The initialization method for the Source. The context contains all the
   * Flume configuration info, and can be used to retrieve any configuration
   * values necessary to set up the Source.
   */
  public void configure(Context context) {
    consumerKey = context.getString(TwitterSourceConstants.CONSUMER_KEY_KEY);
    consumerSecret = context.getString(TwitterSourceConstants.CONSUMER_SECRET_KEY);
    accessToken = context.getString(TwitterSourceConstants.ACCESS_TOKEN_KEY);
    accessTokenSecret = context.getString(TwitterSourceConstants.ACCESS_TOKEN_SECRET_KEY);
    
    String keywordString = context.getString(TwitterSourceConstants.KEYWORDS_KEY, "");
    String locationString = context.getString(TwitterSourceConstants.LOCATION, "");
    
    if (locationString.trim().length() == 0) {
      location = new double[][]{new double[]{-180d, -90d},new double[]{180d, 90d}};
    } else {
      String[] locationStringArray = locationString.split(",");
      location = new double[][]{
          new double[]{Double.parseDouble(locationStringArray[0].trim()), Double.parseDouble(locationStringArray[1].trim())},
          new double[]{Double.parseDouble(locationStringArray[2].trim()), Double.parseDouble(locationStringArray[3].trim())}
        };
    }
    if (keywordString.trim().length() == 0) {
      keywords = new String[0];
    } else {
      keywords = keywordString.split(",");
      for (int i = 0; i < keywords.length; i++) {
        keywords[i] = keywords[i].trim();
      }
    }
    
    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setOAuthConsumerKey(consumerKey);
    cb.setOAuthConsumerSecret(consumerSecret);
    cb.setOAuthAccessToken(accessToken);
    cb.setOAuthAccessTokenSecret(accessTokenSecret);
    cb.setJSONStoreEnabled(true);
    cb.setIncludeEntitiesEnabled(true);
    
    twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
  }
  
  /**
   * Start processing events. This uses the Twitter Streaming API to sample
   * Twitter, and process tweets.
   */
  @Override
  public void start() {
    // The channel is the piece of Flume that sits between the Source and Sink,
    // and is used to process events.
    final ChannelProcessor channel = getChannelProcessor();
    
    final Map<String, String> headers = new HashMap<String, String>();
    
    // The StatusListener is a twitter4j API, which can be added to a Twitter
    // stream, and will execute methods every time a message comes in through
    // the stream.
    StatusListener listener = new StatusListener() {
      // The onStatus method is executed every time a new tweet comes in.
      public void onStatus(Status status) {
         GeoLocation geoLocation = status.getGeoLocation();
        if(geoLocation != null){
          // The EventBuilder is used to build an event using the headers and
          // the raw JSON of a tweet
          logger.debug(status.getUser().getScreenName() + ": " + status.getText());
          
          headers.put("timestamp", String.valueOf(status.getCreatedAt().getTime()));
          String text = status.getText().replaceAll("[\r\n]", " ");
          String lon = String.valueOf(geoLocation.getLongitude());
          String lat = String.valueOf(geoLocation.getLatitude());
          String Saprater = ",";
          
          String line  = new StringBuilder()
                    .append(lon)
                    .append(Saprater)
                    .append(lat)
                    .append(Saprater)
                    .append(status.getCreatedAt().getTime())
                    .append(Saprater)
                    .append(status.getUser().getId())
                    .append(Saprater)
                    .append(text).toString();
          
          //line = {geoLocation.getLongitude()},{geoLocation.getLatitude.()},{status.getCreatedAt().getTime()},{status.getUser.getId},text+"\n"
          
          Event event = EventBuilder.withBody(
              line.getBytes(), headers);
          /*Event event = EventBuilder.withBody(
              DataObjectFactory.getRawJSON(status).getBytes(), headers);*/
        
          channel.processEvent(event);
        }
      }
      
      // This listener will ignore everything except for new tweets
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
      public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
      public void onScrubGeo(long userId, long upToStatusId) {}
      public void onException(Exception ex) {}
      public void onStallWarning(StallWarning warning) {}
    };
    
    logger.debug("Setting up Twitter sample stream using consumer key {} and" +
        " access token {}", new String[] { consumerKey, accessToken });
    // Set up the stream's listener (defined above),
    twitterStream.addListener(listener);
    FilterQuery query = new FilterQuery();
    // Set up a filter to pull out industry-relevant tweets
    if (keywords.length == 0) {
      logger.debug("Starting up Twitter sampling...");
      
      query = query.locations(location);
    } else {
      logger.debug("Starting up Twitter filtering...");
      
      query.track(keywords);
    }
    twitterStream.filter(query);
    super.start();
  }
  
  
  /**
   * Stops the Source's event processing and shuts down the Twitter stream.
   */
  @Override
  public void stop() {
    logger.debug("Shutting down Twitter sample stream...");
    twitterStream.shutdown();
    super.stop();
  }
  

}
