# Linters Docs

The linters list can be easily extended by adding a new linter to the docker image and adding a new script to the `linters` directory.

### How to add a new linter

First, add a linter installation to the docker folder's Dockerfile. For example, to add a linter for the `R` programming language, add the following lines to the `docker/linter/Dockerfile`:

```dockerfile
ARG R_VERSION=4.2.2
RUN curl -O https://cdn.rstudio.com/r/centos-8/pkgs/R-${R_VERSION}-1-1.x86_64.rpm \
  && rpm -i R-${R_VERSION}-1-1.x86_64.rpm \
  && rm R-${R_VERSION}-1-1.x86_64.rpm \
  && ln -s /opt/R/${R_VERSION}/bin/R /usr/local/bin/R \
  && ln -s /opt/R/${R_VERSION}/bin/Rscript /usr/local/bin/Rscript

RUN R -e "install.packages('lintr', repos='http://cran.us.r-project.org')"
```
> Be sure that you bumped the version of the linter image before merging your PR with a new linter in the docker image.

Next, add a script to linters directory. For example, to add a linter for the `R` programming language, add the following script (`lint_r.sh`) to the (`./linters/lint_r.sh`):

```bash
#!/bin/bash
files="$1"
fix_param="$2"
file_format="R"

if [ "$fix_param" = "--fix" ]; then
    echo "😔 Fix command for $file_format files not implemented yet"
    exit 0
else
    # Run the linter
    echo "🔬 Checking $file_format files..."
  ./linters/run_linter.sh  "R -e \"lintr::lint('$files')\""
fi

exit 0
```

Finally, run make command `make lint-r` to build a new docker image and run the linter for the `R` programming language.

### Linter Configurations

To maintain good code style we should have a good style guides. In our case we keep all configs for linters in the docker image and use it for running linters. You can find examples in the (`./docker/linter/configs`) directory.
