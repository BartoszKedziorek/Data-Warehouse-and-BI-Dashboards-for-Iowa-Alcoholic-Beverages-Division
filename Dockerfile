FROM astrocrpublic.azurecr.io/runtime:3.0-4

USER root


# RUN apt update && \
#     apt install wget && \
#     wget https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.23+9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.23_9.tar.gz && \
#     mkdir -p /opt/java/openjdk11 && \
#     tar -xzf OpenJDK11U-jdk_x64_linux_hotspot_11.0.23_9.tar.gz -C /opt/java/openjdk11 --strip-components=1 && \
#     rm OpenJDK11U-jdk_x64_linux_hotspot_11.0.23_9.tar.gz

# Install OpenJDK-17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV HADOOP_CONF_DIR /opt/hadoop/etc/hadoop
ENV GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/secrets/google-api-key.json
RUN export JAVA_HOME && export HADOOP_CONF_DIR 
RUN export PATH=$JAVA_HOME/bin:$PATH