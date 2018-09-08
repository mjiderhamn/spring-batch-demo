package se.jiderhamn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.Resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Mattias Jiderhamn
 */
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, JobConfiguration.class})
public class JobConfigurationTest {
  
  @Autowired
  private JobLauncher jobLauncher;
  
  @Resource(name = JobConfiguration.JOB_PARSE_CALL_LOG)
  private Job parseCallLog;
  
  @Before
  public void setUp() {
    BillDAO.reset();
    PhoneCallDAO.reset();
  }
  
  private String getPath(String resource) throws URISyntaxException {
    return new File(getClass().getResource(resource).toURI()).getAbsolutePath();    
  }

  @Test
  public void parseSmallCallLog() throws Exception {
    final JobExecution jobExecution = jobLauncher.run(parseCallLog, new JobParametersBuilder()
        .addString("filePath", getPath("/basic.txt"))
        .toJobParameters());
    
    assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    assertEquals(8, PhoneCallDAO.findAll().size());
    final List<Bill> allBills = BillDAO.findAll();
    assertEquals(3, allBills.size());
    assertTrue(allBills.stream().allMatch(Bill::isSent));
  }
  
  @Test
  public void parseSmallCallLogWithError() throws Exception {
    final JobExecution jobExecution = jobLauncher.run(parseCallLog, new JobParametersBuilder()
        .addString("filePath", getPath("/error.txt"))
        .toJobParameters());
    
    assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    assertEquals(7, PhoneCallDAO.findAll().size());
    final List<Bill> allBills = BillDAO.findAll();
    assertEquals(3, allBills.size());
    assertTrue(allBills.stream().allMatch(Bill::isSent));
  }
  
  @Test
  public void parseLargeCallLog() throws Exception {
    final JobExecution jobExecution = jobLauncher.run(parseCallLog, new JobParametersBuilder()
        .addString("filePath", getPath("/large.txt"))
        .toJobParameters());
    
    assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    assertEquals(4000, PhoneCallDAO.findAll().size());
    final List<Bill> allBills = BillDAO.findAll();
    assertEquals(150, allBills.size());
    assertTrue(allBills.stream().allMatch(Bill::isSent));
  }
  
  @Test
  public void parseCallLog_manualConfirmationRequired() throws Exception {
    final JobParameters jobParameters = new JobParametersBuilder()
        .addString("filePath", getPath("/basic.txt"))
        .addString("manualApproval", "true", true)
        .toJobParameters();
    

    // Act
    final JobExecution jobExecution = jobLauncher.run(parseCallLog, jobParameters);


    // Assert
    assertEquals(BatchStatus.STOPPED, jobExecution.getStatus());
    assertEquals(3, jobExecution.getStepExecutions().size());
    assertEquals("Bills created", 3, BillDAO.findAll().size());
    assertFalse("No bill sent", BillDAO.findAll().stream().anyMatch(Bill::isSent));
    
    // Try to restart without having manually approved
    final JobExecution executionWithoutApproval = jobLauncher.run(parseCallLog, jobParameters);
    assertEquals(BatchStatus.STOPPED, executionWithoutApproval.getStatus());
    assertEquals(1, executionWithoutApproval.getStepExecutions().size()); // Only deciding step executed
    assertFalse("No bill sent", BillDAO.findAll().stream().anyMatch(Bill::isSent));
    
    // Approve and continue
    ApprovalDAO.setManuallyApproved(getPath("/basic.txt"), true); // Pretend manually approved
    final JobExecution restartExecution = jobLauncher.run(parseCallLog, jobParameters);
    assertEquals(BatchStatus.COMPLETED, restartExecution.getStatus());
    assertEquals(3, restartExecution.getStepExecutions().size()); // Incl deciding step
    assertTrue("All bills sent", BillDAO.findAll().stream().allMatch(Bill::isSent));
  }
  
}