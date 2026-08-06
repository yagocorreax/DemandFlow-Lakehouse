FROM apache/hive:4.0.0

ARG HADOOP_AWS_VERSION=3.3.6
ARG AWS_JAVA_SDK_BUNDLE_VERSION=1.12.367

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && mkdir -p /opt/hive/auxlib \
    && curl -fLsS \
       -o /opt/hive/auxlib/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -fLsS \
       -o /opt/hive/auxlib/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar \
       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER hive
