package se.jiderhamn;

import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author Mattias Jiderhamn
 */
@SuppressWarnings("SameParameterValue")
public class CallLogGenerator {
  
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

  private final Random rand = new Random();
  
  private final List<String> subscribers = new ArrayList<>();

  public static void main(String[] args) {
    System.out.println(new CallLogGenerator()
      .generateSubscribers(150)
      .generateCalls(4000));
  }

  private CallLogGenerator generateSubscribers(int noOfSubscribers) {
    while(subscribers.size() < noOfSubscribers) {
      final int r = rand.nextInt(10000000);
      final String subscriber = "070" + StringUtils.leftPad(Integer.toString(r), 7, "0");
      if(! subscribers.contains(subscriber)) { // (Yuk!)
        subscribers.add(subscriber);
      }
    }
    return this;
  }

  private String generateCalls(int noOfCalls) {
    if(subscribers.isEmpty())
      throw new IllegalStateException("No subscribers!");
    
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < noOfCalls; i++) {
      final int caller = rand.nextInt(subscribers.size());
      int receiver;
      do {
        receiver = rand.nextInt(subscribers.size());
      } while(caller == receiver);
      Duration duration = Duration.ofSeconds(rand.nextInt(4000) + 1);

      sb.append(subscribers.get(caller)).append(" | ").append(subscribers.get(receiver)).append(" | ").append(PhoneCall.MIDNIGHT.plus(duration).format(FORMATTER)).append("\n");
    }
    return sb.toString();
  }

}