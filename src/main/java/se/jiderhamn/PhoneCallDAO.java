package se.jiderhamn;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toSet;

/**
 * @author Mattias Jiderhamn
 */
@SuppressWarnings("WeakerAccess")
public class PhoneCallDAO {
  
  private static List<PhoneCall> calls = new ArrayList<>();

  public static void persist(List<? extends PhoneCall> phoneCalls) {
    calls.addAll(phoneCalls);
  }
  
  public static List<PhoneCall> findAll() {
    return unmodifiableList(calls);
  }
  
  public static List<String> getSubscribers() {
    final Set<String> subscribers = calls.stream()
        .map(PhoneCall::getFromSubscriber)
        .collect(toSet());
    subscribers.addAll(calls.stream()
            .map(PhoneCall::getToSubscriber)
            .collect(toSet()));
    return new ArrayList<>(subscribers);
  }
  
  public static long getTotalNoOfCallsFrom(String subscriber) {
    return calls.stream()
        .filter(call -> subscriber.equals(call.getFromSubscriber()))
        .count();
  }
  
  /** Reset fake repository for next text case */
  public static void reset() {
    calls.clear();
  }
}