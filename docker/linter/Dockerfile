# Use an Alpine-based Python image
FROM python:3.11-alpine

ARG WORKDIR=/usr/src/app
ARG CHECKSTYLE_VERSION=10.12.3

USER root

RUN apk --no-cache add bash curl openjdk11-jdk nodejs npm && \
  wget -O /opt/checkstyle.jar https://github.com/checkstyle/checkstyle/releases/download/checkstyle-${CHECKSTYLE_VERSION}/checkstyle-${CHECKSTYLE_VERSION}-all.jar && \
  echo -e '#!/bin/sh\njava -jar /opt/checkstyle.jar "$@"' > /usr/local/bin/checkstyle && \
  chmod +x /usr/local/bin/checkstyle

WORKDIR ${WORKDIR}

# Install Python dependencies
COPY ./docker/linter/linter-requirements.txt ${WORKDIR}/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install Node.js dependencies
RUN npm install -g eslint --no-optional

# Copy configs for linters
COPY ./docker/linter/configs/ /root/.config/linter/

# Set PATH for linting script
ENV PATH="${WORKDIR}:${PATH}"
