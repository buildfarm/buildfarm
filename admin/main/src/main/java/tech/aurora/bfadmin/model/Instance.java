package tech.aurora.bfadmin.model;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Instance implements Serializable {
  private static final long serialVersionUID = 4343106139005494435L;
  private static final Logger logger = LoggerFactory.getLogger(Instance.class);

  private com.amazonaws.services.ec2.model.Instance ec2Instance;
  private Long containerStartTime;
  private String clusterId;
  private String groupType;
  private String workerType = "";

  public String getUptimeStr() {
    return getFormattedTimestamp(ec2Instance.getLaunchTime().getTime() / 1000);
  }

  public String getContainerUptimeStr() {
    // Do not report container uptime if it hasn't been updated yet
    if (getContainerUptimeInHours() > getUptimeInHours()) {
      return "N/A";
    } else {
      return getFormattedTimestamp(containerStartTime);
    }
  }

  public Long getUptimeInHours() {
    return getTimestampInHours(ec2Instance.getLaunchTime().getTime() / 1000);
  }

  public Long getContainerUptimeInHours() {
    return getTimestampInHours(containerStartTime);
  }

  public String getFormattedLaunchTime() {
    return new SimpleDateFormat("MMM dd hh:mm").format(ec2Instance.getLaunchTime());
  }

  public String getLifecycle() {
    return ec2Instance.getInstanceLifecycle() != null ? ec2Instance.getInstanceLifecycle() : "on demand";
  }

  private String getFormattedTimestamp(Long timestamp) {
    if (timestamp == null || timestamp == 0) {
      return "N/A";
    } else {
      Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      Long formattedTimestamp = (cal.getTimeInMillis() / 1000 - timestamp) / 60;
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
