package se.jiderhamn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * @author Mattias Jiderhamn
 */
@SuppressWarnings({"SpringElInspection", "SpringJavaAutowiredFieldsWarningInspection", "SpringJavaInjectionPointsAutowiringInspection"})
@Configuration
public class JobConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger("JobConfiguration");

  @Autowired
  private StepBuilderFactory steps;
  
  @Autowired
  private JobBuilderFactory jobs;

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  
  @Bean(name = "parseCallLogJob")
  protected Job parseCallLogJob() {
    return jobs.get("parseCallLog")
        .validator(new DefaultJobParametersValidator(new String[] {"filePath"}, new String[] {"manualApproval"}))
        .start(readCallDataFromFile())
        .next(createBills())
        .next(stopForManualApproval( /* Overridden by expression */))
        .next(sendBills())
        .next(notifyDone())
        .listener(new JobExecutionListener() {
          @Override
          public void beforeJob(JobExecution jobExecution) {
            LOG.info("Starting job {}, with parameters {}", jobExecution.getJobInstance(), jobExecution.getJobParameters());
          }

          @Override
          public void afterJob(JobExecution jobExecution) {
            final String path = jobExecution.getJobParameters().getString("filePath");
            // NOTE! Comparison must be made on exitCode only, which compareTo() does
            if(ExitStatus.COMPLETED.compareTo(jobExecution.getExitStatus()) == 0) {
              LOG.info("Job completed successfully for file " + path);
            }  
            else if(ExitStatus.FAILED.compareTo(jobExecution.getExitStatus()) == 0) {
              LOG.info("Job failed for file " + path);
            }  
            else if(ExitStatus.STOPPED.compareTo(jobExecution.getExitStatus()) == 0) {
              LOG.error("Job stopped - file " + path);
            }
            else {
              // ExitStatus.UNKNOWN, ExitStatus.EXECUTING, ExitStatus.NOOP
              LOG.error("Job exited with status {}", jobExecution.getExitStatus());
            }
          }
        })
        .build();
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Bean
  Step readCallDataFromFile() {
    return steps.get("readCallDataFromFile")
        .<PhoneCall, PhoneCall>chunk(100) // Commit-limit
        .faultTolerant()
          .skip(FlatFileParseException.class).skipLimit(10)
        .reader(flatFileReader("Overridden by expression"))
        .writer(PhoneCallDAO::persist)
        .listener(new ItemReadListener<PhoneCall> () {
          @Override
          public void beforeRead() {
            // LOG.info("beforeRead()");
          }

          @Override
          public void afterRead(PhoneCall item) {
            // LOG.info("afterRead() " + item);
          }

          @Override
          public void onReadError(Exception ex) {
            LOG.error("Error reading", ex);
          }
        })
        .listener(new SkipListener<PhoneCall, PhoneCall>() {
          @Override
          public void onSkipInRead(Throwable t) {
            LOG.error("Skip reading call log", t);
          }

          @Override
          public void onSkipInWrite(PhoneCall item, Throwable t) {
            LOG.error("Skip writing call " + item, t);
          }

          @Override
          public void onSkipInProcess(PhoneCall item, Throwable t) {
            LOG.error("Skip processing call" + item, t);
          }
        })
        
        .listener(new ItemWriteListener<PhoneCall>() {

          @Override
          public void beforeWrite(List item) {
            // LOG.info("beforeWrite() " + item);
          }

          @Override
          public void afterWrite(List item) {
            // LOG.info("afterWrite() " + item);
          }

          @Override
          public void onWriteError(Exception ex, List items) {
            LOG.error("onWriteError() " + items, ex);
          }
        })
        .listener(new StepExecutionListener() {
          @Override
          public void beforeStep(StepExecution stepExecution) {
            // *Before* tx started
            LOG.info("Here we can check pre-conditions (such as duplicate prevention) and log progress");
          }

          @Override
          public ExitStatus afterStep(StepExecution stepExecution) {
            return null;
          }
        })
        .build();
  }

  @Bean
  @JobScope // Needed for @Value
  FlatFileItemReader<PhoneCall> flatFileReader(@Value("#{jobParameters[filePath]}") String filePath) {
    return new FlatFileItemReaderBuilder<PhoneCall>()
        .name("callLogReader")
        .resource(new FileSystemResource(filePath))
        .delimited().delimiter("|")
        .names(new String[] {"fromSubscriber", "toSubscriber", "duration"})
        .targetType(PhoneCall.class)
        .build();
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Bean
  protected Step createBills() {
    return steps.get("createBills")
        .<String, Bill>chunk(100)
        .faultTolerant()
          .retry(TimeoutException.class)
          .retryLimit(10)
          .backOffPolicy(new ExponentialBackOffPolicy())
        .reader(phoneCallReader())
        .processor(createBillsProcessor())
        .listener(new ItemProcessListener<String, Bill>() {
          @Override
          public void beforeProcess(String item) {
            LOG.info("beforeProcess() " + item);
          }

          @Override
          public void afterProcess(String item, Bill result) {
            LOG.info("afterProcess() " + item + " => " + result);
          }

          @Override
          public void onProcessError(String item, Exception e) {
            if(e instanceof TimeoutException)
              LOG.info("onProcessError: Timed out processing " + item + " - will retry");
            else
              LOG.error("onProcessError: " + item, e);
          }
        })
        .writer(BillDAO::persist)
        .build();
  }
  
  @Bean
  @StepScope // Needed for postponed DAO invocation
  ItemReader<String> phoneCallReader() {
    return new ListItemReader<>(PhoneCallDAO.getSubscribers());
  }

  private ItemProcessor<? super String, ? extends Bill> createBillsProcessor() {
    return subscriber -> {
      if(Math.random() < 0.01) // Lower for larger file
        throw new TimeoutException();
      
      final long noOfCalls = PhoneCallDAO.getTotalNoOfCallsFrom(subscriber);
      if(noOfCalls > 0) {
        final Duration totalDuration = PhoneCallDAO.getTotalDurationOfCallsFrom(subscriber);
        return new Bill(subscriber, noOfCalls, totalDuration);
      }
      else
        return null; // Skip
    };
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Bean
  protected Step stopForManualApproval() {
    return steps.get("stopForManualApprovalStep")
        .flow(new FlowBuilder<SimpleFlow>("stopForManualApprovalFlow")
          .start(decideOnManualApproval()).on(FlowExecutionStatus.STOPPED.getName()).stopAndRestart(sendBills())
          .from(decideOnManualApproval()).on("*").to(sendBills())
          .build())
        .build();
  }

  private JobExecutionDecider decideOnManualApproval() {
    return (jobExecution, stepExecution) -> {
      final String filePath = jobExecution.getJobParameters().getString("filePath");
      final boolean manualApproval = Boolean.parseBoolean(jobExecution.getJobParameters().getString("manualApproval", "false"));
      if(manualApproval && ! ApprovalDAO.isManuallyApproved(filePath)) {
        return FlowExecutionStatus.STOPPED;
      }
      else
        return FlowExecutionStatus.COMPLETED;
    };
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Bean
  Step sendBills() {
    return steps.get("sendBills")
        .<Bill, Bill>chunk(100)
        .reader(billReader())
        .processor((ItemProcessor<Bill, Bill>) Bill::send) // NOTE! This should be idempotent!
        .writer(items -> { }) // No writing - storing is expected to happen in processor
        .listener(new ItemReadListener<Bill>() {
          @Override
          public void beforeRead() {
            LOG.info("Before reading bills");
          }

          @Override
          public void afterRead(Bill item) {
            LOG.warn("sendBills read " + item);
          }

          @Override
          public void onReadError(Exception ex) {

          }
        })
        .listener(new SkipListener<Bill, Bill>() {
          @Override
          public void onSkipInRead(Throwable t) {
            LOG.error("Error reading bill - skipping", t);
          }

          @Override
          public void onSkipInWrite(Bill item, Throwable t) {
            LOG.error("Error writing bill - skipping " + item, t);
          }

          @Override
          public void onSkipInProcess(Bill item, Throwable t) {
            LOG.error("Error processing bill - skipping" + item, t);
          }
        })
        .build();
  }

  @Bean
  @StepScope // Needed for postponed DAO invocation
  ListItemReader<Bill> billReader() {
    return new ListItemReader<>(BillDAO.findAll());
  }

  @Bean
  protected Step notifyDone() {
    return steps.get("notifyDoneStep")
        .tasklet((contribution, chunkContext) -> {
          LOG.info("Pretend that we're sending an e-mail");
          return RepeatStatus.FINISHED;
        })
        .build();
  }

}