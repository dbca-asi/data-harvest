# Prepare the base environment.
#FROM python:3.7-slim-buster as builder_base_rt
FROM osgeo/gdal:ubuntu-full-3.0.4 as builder_base_rt
MAINTAINER asi@dbca.wa.gov.au
RUN apt-get update -y \
  && apt-get upgrade -y \
  && apt-get install --no-install-recommends -y vim wget git telnet libmagic-dev gcc binutils libproj-dev python3-dev python3-pip python3-setuptools libpq-dev\
  && rm -rf /var/lib/apt/lists/* \
  && pip3 install --upgrade pip


RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash
RUN az extension add --name log-analytics

RUN ln -s /usr/bin/python3 /usr/bin/python
# Install Python libs from requirements.txt.
FROM builder_base_rt as python_libs_rt
WORKDIR /app
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

# Install the project.
FROM python_libs_rt
COPY *.py ./
COPY db ./db
COPY files ./files
COPY utils ./utils
COPY resource_tracking ./resource_tracking
COPY docker ./docker
COPY nginx ./nginx
COPY rancher ./rancher
COPY azlog ./azlog
COPY nginxlog ./nginxlog
COPY containerstatus ./containerstatus
COPY podstatus ./podstatus

# Run the application as the www-data user.
USER nobody
CMD ["/bin/bash"]
