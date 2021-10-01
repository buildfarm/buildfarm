FROM java:8
USER root
VOLUME /tmp
VOLUME /var/lib
ADD bfadmin.jar bfadmin.jar
ENTRYPOINT ["java", "-jar", "-Dspring.config.location=classpath:/,file:/var/lib/", "/bfadmin.jar"]
EXPOSE 8080