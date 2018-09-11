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

  public static Instances singular(Instance instance) {
    return new Instances() {
      @Override
      public Instance getFromBlob(String blobName) throws InstanceNotFoundException { return instance; }

      @Override
      public Instance getFromUploadBlob(String uploadBlobName) throws InstanceNotFoundException { return instance; }

      @Override
      public Instance getFromOperationsCollectionName(String operationsCollectionName) throws InstanceNotFoundException { return instance; }

      @Override
      public Instance getFromOperationName(String operationName) throws InstanceNotFoundException { return instance; }

      @Override
      public Instance getFromOperationStream(String operationStream) throws InstanceNotFoundException { return instance; }

      @Override
      public Instance get(String name) throws InstanceNotFoundException { return instance; }

      @Override
      public void start() { }

      @Override
      public void stop() { }
    };
  }
}
