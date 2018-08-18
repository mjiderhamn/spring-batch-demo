package se.jiderhamn;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * @author Mattias Jiderhamn
 */
@SuppressWarnings("WeakerAccess")
public class BillDAO {
  
  private static List<Bill> bills = new ArrayList<>();
  
  /** Reset fake repository for next text case */
  public static void reset() {
    bills.clear();
  }
  
  public static List<Bill> findAll() {
    return unmodifiableList(bills);
  }

  public static void persist(List<? extends Bill> phoneCalls) {
    bills.addAll(phoneCalls);
  }
  
}