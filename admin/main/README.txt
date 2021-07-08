# Buildfarm Admin

## Usage

### Build and Run Locally

```
./mvnw clean package && java -jar target/bfadmin.jar -Dspring.config.location=file:./
```

### Build and Run Docker Container

```
./mvnw clean package docker:build -DdockerImageTags=1.0
docker run -p 8080:8080 -v $HOME:/var/lib -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN bazelbuild/buildfarm-admin:1.0
```