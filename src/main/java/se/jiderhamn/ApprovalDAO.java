package se.jiderhamn;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Mattias Jiderhamn
 */
@SuppressWarnings("WeakerAccess")
public class ApprovalDAO {
  
  private static final Map<String, Boolean> approved = new HashMap<>();
  
  public static void setManuallyApproved(String key, boolean value) {
    approved.put(key, value);
  }
  
  public static boolean isManuallyApproved(String key) {
    return approved.getOrDefault(key, Boolean.FALSE);
  }
  
}