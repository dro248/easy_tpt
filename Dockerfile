FROM fedora:34

RUN yum install -y java-11-openjdk-devel
ENV PATH=$PATH:/usr/java/latest/bin
ENV JAVA_HOME=/usr/java/latest

# TODO: Copy directly from source?
COPY helpers/TeradataToolsAndUtilitiesBase__linux_x8664.17.10.09.00.tar.gz ./ttu.tar.gz

RUN tar xfz ttu.tar.gz && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/azureaxsmod1700-17.00.00.06-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/bteq1700-17.00.00.06-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/cliv21700-17.00.00.31-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/fastexp1700-17.00.00.10-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/fastld1700-17.00.00.10-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/gcsaxsmod1700-17.00.00.03-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/jmsaxsmod1700-17.00.00.08-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/mload1700-17.00.00.08-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/mqaxsmod1700-17.00.00.08-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/npaxsmod1700-17.00.00.07-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/piom1700-17.00.00.08-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/s3axsmod1700-17.00.00.09-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/sqlpp1700-17.00.00.04-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/tdicu1700-17.00.00.23-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/tdodbc1700-17.00.00.26-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/tdwallet1700-17.00.00.53-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/teragssAdmin1700-17.00.00.09-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/tptbase1700-17.00.00.18-1.x86_64.rpm && \
  rpm -ivh --nodeps TeradataToolsAndUtilitiesBase/Linux/x8664/tpump1700-17.00.00.08-1.x86_64.rpm && \
  dnf -y install libnsl unzip && \
  curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
  unzip awscliv2.zip  && \
  sudo ./aws/install

# Change the working directory
WORKDIR "/tpt"

# Install Python
RUN yum install -y python3-pip
RUN pip3 install poetry

# Install Python packages with Poetry
COPY helpers/pyproject.toml .
RUN poetry install

# Copy job/variable Jinja templates
COPY templates ./templates

# Copy in the `run_job.py` package that will compile and run our job
COPY helpers/run_job.py .

# Run the Docker container as an executable
ENTRYPOINT ["poetry", "run", "python", "run_job.py"]