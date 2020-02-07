FROM apache/nifi

WORKDIR /opt/
COPY build/*-runner /opt/microservice
RUN chmod 775 /opt

CMD ["/opt/microservice", "-Dquarkus.http.host=0.0.0.0"]
