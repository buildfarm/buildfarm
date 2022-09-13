package tech.aurora.bfadmin.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tech.aurora.bfadmin.service.AdminService;
import org.springframework.beans.factory.annotation.Autowired;

@Component
@EnableScheduling
@EnableAsync
public class ScheduledTasks {
  private static final Logger logger = LoggerFactory.getLogger(ScheduledTasks.class);

  @Autowired
  AdminService adminService;

  @Value("${buildfarm.scheduled.indexer}")
  private boolean indexerTaskEnabled;

  @Async
  @Scheduled(cron = "${buildfarm.scheduled.indexer.cron}")
  public void runWorkerIndexer() {
    if (adminService.isPrimaryAdminHost() && indexerTaskEnabled) {
      logger.info("Running worker indexer...");
      adminService.reindexCas();
    }
  }
}
