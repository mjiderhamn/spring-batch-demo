package se.jiderhamn;

import java.time.Duration;
import java.time.LocalTime;

/**
 * @author Mattias Jiderhamn
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class PhoneCall {

  private static final LocalTime MIDNIGHT = LocalTime.of(0, 0);

  private String fromSubscriber;
  
  private String toSubscriber;
  
  // TODO Start to have limit
  
  private Duration duration;

  public PhoneCall() {
  }

  public PhoneCall(String fromSubscriber, String toSubscriber, Duration duration) {
    this.fromSubscriber = fromSubscriber;
    this.toSubscriber = toSubscriber;
    this.duration = duration;
  }

  public String getFromSubscriber() {
    return fromSubscriber;
  }

  public void setFromSubscriber(String fromSubscriber) {
    this.fromSubscriber = fromSubscriber;
  }

  public String getToSubscriber() {
    return toSubscriber;
  }

  public void setToSubscriber(String toSubscriber) {
    this.toSubscriber = toSubscriber;
  }

  public Duration getDuration() {
    return duration;
  }

  public void setDuration(String durationString) {
    this.duration = Duration.between(MIDNIGHT, LocalTime.parse(durationString));
  }

  @Override
  public String toString() {
    return "PhoneCall[" + fromSubscriber + " -> " + toSubscriber + " " + duration + "]";
  }
}