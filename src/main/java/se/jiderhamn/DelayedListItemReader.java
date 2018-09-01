package se.jiderhamn;

import org.springframework.batch.item.ItemReader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Variant of {@link org.springframework.batch.item.support.ListItemReader} that gets the list from a {@link Supplier} when it is needed,
 * which means it isn't fetched during step creation, but postponed until step execution.
 * @author Mattias Jiderhamn
 */
public class DelayedListItemReader<T> implements ItemReader<T> {
  
  private final Supplier<List<T>> supplier;
  
  private List<T> list;

  public DelayedListItemReader(Supplier<List<T>> supplier) {
    this.supplier = supplier;
  }
  
  private List<T> getList() {
    if(list == null) {
      this.list = new ArrayList<>(supplier.get()); // Copy so that we can remove
    }
    return list;
  }

  public T read() {
    return ! getList().isEmpty() ? getList().remove(0) : null;
  }
}