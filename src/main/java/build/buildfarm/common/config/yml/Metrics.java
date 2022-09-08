package build.buildfarm.common.config.yml;

public class Metrics {
  private String publisher = "log";
  private String logLevel = "FINE";

  private String topic;

  private int topicMaxConnections;

  private String secretName;

  public String getLogLevel() {
    return logLevel;
  }

  public void setLogLevel(String logLevel) {
    this.logLevel = logLevel;
  }

  public String getPublisher() {
    return publisher;
  }

  public void setPublisher(String publisher) {
    this.publisher = publisher;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public int getTopicMaxConnections() {
    return topicMaxConnections;
  }

  public void setTopicMaxConnections(int topicMaxConnections) {
    this.topicMaxConnections = topicMaxConnections;
  }

  public String getSecretName() {
    return secretName;
  }

  public void setSecretName(String secretName) {
    this.secretName = secretName;
  }

  @Override
  public String toString() {
    return "Metrics{"
        + "publisher='"
        + publisher
        + '\''
        + ", logLevel='"
        + logLevel
        + '\''
        + ", topic='"
        + topic
        + '\''
        + ", topicMaxConnections="
        + topicMaxConnections
        + ", secretName='"
        + secretName
        + '\''
        + '}';
  }
}
