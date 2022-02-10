package tech.aurora.bfadmin.model;

import java.io.Serializable;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClusterInfo implements Serializable {
  private static final long serialVersionUID = 4343106139005494435L;
  private static final Logger logger = LoggerFactory.getLogger(ClusterInfo.class);

  private String clusterId;
  private Asg servers;
  private List<Asg> workers;
}
