package se.jiderhamn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * @author Mattias Jiderhamn
 */
@Configuration
@EnableBatchProcessing
public class BatchTestConfiguration extends DefaultBatchConfigurer {
  
  private static final Logger LOG = LoggerFactory.getLogger("TX");

  private final PlatformTransactionManager transactionManager = new AbstractPlatformTransactionManager() {
    @Override
    protected Object doGetTransaction() throws TransactionException {
      return "dummy";
    }

    @Override
    protected void doBegin(Object o, TransactionDefinition transactionDefinition) throws TransactionException {
      LOG.info("Beginning transaction");
    }

    @Override
    protected void doCommit(DefaultTransactionStatus defaultTransactionStatus) throws TransactionException {
      LOG.info("Committing transaction");
    }

    @Override
    protected void doRollback(DefaultTransactionStatus defaultTransactionStatus) throws TransactionException {
      LOG.info("Rolling transaction back");
    }
  };
  
  @Override
  public PlatformTransactionManager getTransactionManager() {
    return transactionManager;
  }

}