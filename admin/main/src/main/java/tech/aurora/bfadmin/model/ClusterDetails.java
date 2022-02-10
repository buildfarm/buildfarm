package tech.aurora.bfadmin.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClusterDetails implements Serializable {
  private static final long serialVersionUID = 4343106139005494435L;
  private static final Logger logger = LoggerFactory.getLogger(ClusterDetails.class);

  private String clusterId;
  private List<Instance> servers;
  private List<Instance> workers;

  public List<Instance> getWorkers() {
    Collections.sort(workers, Comparator.comparing(Instance::getWorkerType).thenComparing(Instance::getUptimeInHours, Comparator.reverseOrder()));
    return workers;
  }
}
