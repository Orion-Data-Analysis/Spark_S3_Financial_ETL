FROM apache/spark:3.5.0

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

USER root

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/spark/jars/

RUN chmod 0644 /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
    && mkdir -p /opt/bitnami \
    && ln -s /opt/spark /opt/bitnami/spark

USER spark
