package build.buildfarm.server;

import build.buildfarm.instance.Instance;

public interface Instances {
  void start();
  void stop();
  Instance getFromBlob(String blobName) throws InstanceNotFoundException;
  Instance getFromUploadBlob(String uploadBlobName) throws InstanceNotFoundException;
  Instance getFromOperationsCollectionName(String operationsCollectionName) throws InstanceNotFoundException;
  Instance getFromOperationName(String operationName) throws InstanceNotFoundException;
  Instance getFromOperationStream(String operationStream) throws InstanceNotFoundException;
  Instance get(String name) throws InstanceNotFoundException;
}
