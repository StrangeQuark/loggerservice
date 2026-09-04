# Stage 1: Build the application
FROM eclipse-temurin:21-alpine@sha256:6ea5548706b60ac0a602eaf48af74792cbab012d90e811ca8db6184b16b5c3d6 AS builder

WORKDIR /loggerservice

COPY .mvn/ .mvn
COPY mvnw pom.xml ./
RUN chmod +x mvnw && sed -i 's/\r$//' mvnw
RUN ./mvnw dependency:go-offline

COPY src ./src
RUN ./mvnw clean package

# Stage 2: Create minimal runtime image
FROM eclipse-temurin:21-alpine@sha256:6ea5548706b60ac0a602eaf48af74792cbab012d90e811ca8db6184b16b5c3d6

RUN apk add --no-cache curl

WORKDIR /loggerservice

COPY --from=builder /loggerservice/target/*.jar loggerservice.jar

ENV JAVA_OPTS=""

EXPOSE 6030

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar loggerservice.jar"]
