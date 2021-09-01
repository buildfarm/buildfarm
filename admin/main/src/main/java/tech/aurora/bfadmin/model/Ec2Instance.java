package tech.aurora.bfadmin.model;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Ec2Instance implements Serializable {
  private static final long serialVersionUID = 4343106139005494435L;
  private static final Logger logger = LoggerFactory.getLogger(Ec2Instance.class);

  private Long instanceNumber;
  private String instanceId;
  private String privateIp;
  private Date launchTime;
  private String state;
  private String privateDns;
  private Long uptime;
  private Long containerStartTime;
  private Long numCores;
  private String instanceType;
  private String lifecycle;

  public String getState() {
    return state != null ? state.toUpperCase() : "N/A";
  }

  public String getUptimeStr() {
    return getFormattedTimestamp(uptime / 1000);
  }

  public String getContainerUptimeStr() {
    if (getFormattedTimestamp(containerStartTime) > getFormattedTimestamp(uptime / 1000)) {
      return "N/A";
    } else {
      return getFormattedTimestamp(containerStartTime);
    }
  }

  public Long getUptimeInHours() {
    return getTimestampInHours(uptime / 1000);
  }

  public Long getContainerUptimeInHours() {
    return getTimestampInHours(containerStartTime);
  }

  public String getFormattedLaunchTime() {
    return new SimpleDateFormat("MMM dd hh:mm").format(launchTime);
  }

  public String getLifecycle() {
    return lifecycle == null ? "on demand" : lifecycle;
  }

  private String getFormattedTimestamp(Long timestamp) {
    if (timestamp == null || timestamp == 0) {
      return "N/A";
    } else {
      Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      Long formattedTimestamp = (cal.getTimeInMillis() / 1000 - timestamp) / 60;
      logger.info("getFormattedTimestamp initial value: {}, final value: {} minutes", timestamp,
          formattedTimestamp);
      if (formattedTimestamp < 120) { // 2 hours
        return formattedTimestamp + " Minutes";
      } else if (formattedTimestamp < 2880) { // 2 days
        return formattedTimestamp / 60 + " Hours";
      } else {
        return formattedTimestamp / 1440 + " Days";
      }
    }
  }

  private Long getTimestampInHours(Long timestamp) {
    if (timestamp == null || timestamp == 0) {
      return 0L;
    } else {
      Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      return (cal.getTimeInMillis() / 1000 - timestamp) / 3600;
    }
  }
}
