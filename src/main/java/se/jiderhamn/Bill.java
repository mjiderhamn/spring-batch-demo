package se.jiderhamn;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * @author Mattias Jiderhamn
 */
@SuppressWarnings({"WeakerAccess", "FieldCanBeLocal", "unused"})
public class Bill {
  
  private String subscriber;
  
  private long noOfCalls;
  
  private Duration totalDuration;
  
  private BigDecimal amount;
  
  private boolean sent = false;

  public Bill(String subscriber, long noOfCalls, Duration totalDuration, BigDecimal amount) {
    this.subscriber = subscriber;
    this.noOfCalls = noOfCalls;
    this.totalDuration = totalDuration;
    this.amount = amount;
  }

  public Bill send() {
    sent = true; // Pretend to be sent
    return this;
  }

  public boolean isSent() {
    return sent;
  }
}