package se.jiderhamn;

import java.time.Duration;

/**
 * @author Mattias Jiderhamn
 */
@SuppressWarnings("WeakerAccess")
public class PhoneCall {
  
  private String fromSubscriber;
  
  private String toSubscriber;
  
  // TODO Start to have limit
  
  private Duration duration;

  public PhoneCall(String fromSubscriber, String toSubscriber, Duration duration) {
    this.fromSubscriber = fromSubscriber;
    this.toSubscriber = toSubscriber;
    this.duration = duration;
  }

  public String getFromSubscriber() {
    return fromSubscriber;
  }

  public String getToSubscriber() {
    return toSubscriber;
  }

  public Duration getDuration() {
    return duration;
  }

  @Override
  public String toString() {
    return "PhoneCall[" + fromSubscriber + " -> " + toSubscriber + " " + duration + "]";
  }
}