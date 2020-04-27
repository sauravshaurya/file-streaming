# Step: Build the package
#FROM maven:3.5.3-jdk-8-alpine AS build
#WORKDIR /build
#COPY pom.xml .
#COPY /lib ./lib
#RUN mvn dependency:go-offline
#
#COPY . .
#RUN mvn clean package
#
## Step: Package image
#FROM confluentinc/cp-kafka-connect:5.4.0
#COPY --from=build /build/target/file-streaming-1.0.0.jar /usr/share/confluent-hub-components/


FROM confluentinc/cp-kafka-connect:5.4.1-1-ubi8
USER appuser
COPY target/file-streaming-1.0.0.jar /usr/share/confluent-hub-components/