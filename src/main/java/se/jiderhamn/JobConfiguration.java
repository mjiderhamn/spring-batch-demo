package se.jiderhamn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
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