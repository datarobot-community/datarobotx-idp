# Use an Alpine-based Python image
FROM python:3.11-alpine

ARG WORKDIR=/usr/src/app

USER root
RUN apk --no-cache add bash curl git
WORKDIR ${WORKDIR}

# Get all dependencies required for project
# this need for propper run of linters like mypy, pylint, etc.
COPY ./dev-requirements.txt ${WORKDIR}/dev-requirements.txt
COPY ./requirements.txt ${WORKDIR}/project-requirements.txt
# Install Python dependencies
RUN pip install --no-cache-dir -r dev-requirements.txt -r project-requirements.txt

# Copy configs for linters
COPY ./docker/linter/configs/ /root/.config/linter/
RUN git config --global --add safe.directory '*'
# Set PATH for linting script
ENV PATH="${WORKDIR}:${PATH}"
