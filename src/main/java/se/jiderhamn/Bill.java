package se.jiderhamn;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * @author Mattias Jiderhamn
 */
@SuppressWarnings({"WeakerAccess", "FieldCanBeLocal", "unused"})
public class Bill {

  private static final BigDecimal COST_PER_CALL = new BigDecimal("0.5");
  
  private static final BigDecimal COST_PER_SECOND = new BigDecimal("0.0125");
  
  private String subscriber;
  
  private long noOfCalls;
  
  private Duration totalDuration;
  
  private BigDecimal amount;
  
  private boolean sent = false;

  public Bill(String subscriber, long noOfCalls, Duration totalDuration) {
    this.subscriber = subscriber;
    this.noOfCalls = noOfCalls;
    this.totalDuration = totalDuration;
    this.amount = COST_PER_CALL.multiply(new BigDecimal(noOfCalls))
                .add(COST_PER_SECOND.multiply(new BigDecimal(totalDuration.getSeconds())));
  }

  public Bill send() {
    sent = true; // Pretend to be sent
    return this;
  }

  public boolean isSent() {
    return sent;
  }

  @Override
  public String toString() {
    return "Bill{" +
        "subscriber='" + subscriber + '\'' +
        ", noOfCalls=" + noOfCalls +
        ", totalDuration=" + totalDuration +
        ", amount=" + amount +
        ", sent=" + sent +
        '}';
  }
}