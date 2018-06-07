package se.jiderhamn;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.item.ItemProcessor;
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

/**
 * @author Mattias Jiderhamn
 */
@SuppressWarnings({"SpringElInspection", "SpringJavaAutowiredFieldsWarningInspection", "SpringJavaInjectionPointsAutowiringInspection"})
@Configuration
public class JobConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(JobConfiguration.class);
  
  static final String JOB_PARSE_CALL_LOG = "parseCallLogJob";

  /** No of rows per database transaction */
  private static final int CHUNK_SIZE = 100;
  
  private static final LocalTime MIDNIGHT = LocalTime.of(0, 0);

  @Autowired
  private JobBuilderFactory jobs;

  @Autowired
  private StepBuilderFactory steps;

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  
  @Bean(name = JOB_PARSE_CALL_LOG)
  protected Job parseCallLogJob() {
    return jobs.get(JOB_PARSE_CALL_LOG)
        // .preventRestart()
        .validator(new DefaultJobParametersValidator(new String[] {"filePath"}, new String[] {"manualApproval"}))
        .start(readCallDataFromFile("OVERRIDDEN_BY_EXPRESSION"))
        .next(createBills())
//        .next(decideOnManualApproval()).on(FlowExecutionStatus.STOPPED.getName()).stopAndRestart(sendBills())
//        .from(decideOnManualApproval()).on("*").to(sendBills())
//        .end()
        .next(stopForManualApproval( /* Overridden by expression */))
        .next(sendBills())
        .next(notifyDone())
        .listener(new JobExecutionListener() {
          @Override
          public void beforeJob(JobExecution jobExecution) {
            final String path = jobExecution.getJobParameters().getString("filePath");
            LOG.info("Starting job {}, with file {}", jobExecution.getJobInstance(), path);
          }

          @Override
          public void afterJob(JobExecution jobExecution) {
            final String path = jobExecution.getJobParameters().getString("filePath");
            // TODO Add job / execution ID to messages
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
  @JobScope // Needed for @Value (Alternatively, @StepScope)
  Step readCallDataFromFile(@Value("#{jobParameters[filePath]}") String filePath) {
    return steps.get("readCallDataFromFile")
        .<PhoneCall, PhoneCall>chunk(CHUNK_SIZE) // Commit-limit
        .faultTolerant()
          .skip(FlatFileParseException.class).skipLimit(10)
//          .retry(SQLTimeoutException.class).retryLimit(10).backOffPolicy(new ExponentialBackOffPolicy())
        .reader(flatFileReader(filePath))
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
            return null; // TODO Example with non-null
          }
        })
        .build();
  }

  private FlatFileItemReader<PhoneCall> flatFileReader(String filePath) {
    return new FlatFileItemReaderBuilder<PhoneCall>()
        .name("callLogReader")
        .resource(new FileSystemResource(filePath))
        .delimited().delimiter("|")
        .names(new String[] {"from", "to", "duration"})
        .fieldSetMapper(fieldSet -> new PhoneCall(
            fieldSet.readString("from"), 
            fieldSet.readString("to"),
            Duration.between(MIDNIGHT, LocalTime.parse(fieldSet.readString("duration")))))
        .build();
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Bean
  @JobScope // Alternatively, @StepScope
  protected Step createBills() { // TODO Parameterize month
    return steps.get("createBills")
        .<String, Bill>chunk(CHUNK_SIZE)
        .reader(new ListItemReader<>(PhoneCallDAO.getSubscribers()))
        .processor(createBillsProcessor())
        .listener(new ItemProcessListener<PhoneCall, PhoneCall>() {
          @Override
          public void beforeProcess(PhoneCall item) {
            LOG.info("beforeProcess() " + item);
          }

          @Override
          public void afterProcess(PhoneCall item, PhoneCall result) {
            LOG.info("beforeProcess() " + item);
          }

          @Override
          public void onProcessError(PhoneCall item, Exception e) {
            LOG.error("onProcessError() " + item, e);
          }
        })
        .writer(BillDAO::persist) // TODO noOp example?
        .build();
  }

  private ItemProcessor<? super String, ? extends Bill> createBillsProcessor() {
    return subscriber -> {
      final long noOfCalls = PhoneCallDAO.getTotalNoOfCallsFrom(subscriber);
      if(noOfCalls > 0) {
        return new Bill(subscriber, noOfCalls, null, BigDecimal.TEN); // TODO
      }
      else
        return null; // Skip
    };
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Bean
  @JobScope
  protected Step stopForManualApproval() {
    // TODO Other flow control example
    /*
    SimpleFlow flow2 = new SimpleFlow("stopForManualApprovalFlow");
    flow2.setStateTransitions(Collections.singletonList(StateTransition.createEndStateTransition(new DecisionState(
        decider, "stopForManualApprovalState"))));
    */

    return steps.get("stopForManualApprovalStep")
        .flow(new FlowBuilder<SimpleFlow>("stopForManualApprovalFlow")
          .start(decideOnManualApproval())
          .on("*").end()
//          .start(decideOnManualApproval()).on(FlowExecutionStatus.STOPPED.getName()).stopAndRestart(sendBills())
//          .from(decideOnManualApproval()).on("*").to(sendBills())
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

  /** Step that composes updated AggregateParty from data source/external registry */
  @Bean
  @JobScope
  Step sendBills() {
    return steps.get("sendBills")
        .<Bill, Bill>chunk(CHUNK_SIZE)
        .reader(new ListItemReader<>(BillDAO.findAll()))
        .processor((ItemProcessor<Bill, Bill>) Bill::send)
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
  protected Step notifyDone() {
    return steps.get("notifyDoneStep")
        .tasklet((contribution, chunkContext) -> {
          LOG.info("Pretend that we're sending an e-mail");
          return RepeatStatus.FINISHED;
        })
        .build();
  }

}