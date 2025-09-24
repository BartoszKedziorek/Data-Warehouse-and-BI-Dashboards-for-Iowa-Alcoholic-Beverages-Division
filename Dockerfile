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

RUN apt-get install -y libodbc2

# new local
RUN apt-get update && apt-get install -y locales \
    locales-all && \
    locale-gen en_US.UTF-8  
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en 
ENV LC_ALL en_US.UTF-8  
RUN update-locale LANG=en_US.UTF-8

COPY install-mcs-odbc-driver.sh ./install-mcs-odbc-driver.sh
RUN ./install-mcs-odbc-driver.sh

# MS SQL Server connection
# RUN apt-get update && \
#     apt-get install -y gnupg && \
#     curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
#     curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
#     apt-get update && ACCEPT_EULA=Y apt install unixodbc jq msodbcsql18 mssql-tools18 unixodbc-dev -y
# RUN locale-gen en_US && \
#     locale-gen en_US.UTF-8 && \
#     update-locale

