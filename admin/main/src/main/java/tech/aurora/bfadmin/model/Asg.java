package tech.aurora.bfadmin.model;

import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Asg implements Serializable {
  private static final long serialVersionUID = 4343106139005494435L;
  private static final Logger logger = LoggerFactory.getLogger(Asg.class);

  private AutoScalingGroup asg;
  private String groupType;
  private String workerType;

  public String getWorkerType() {
    return workerType != null ? workerType.toUpperCase() : "";
  }
}
