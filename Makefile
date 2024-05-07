# lint:
# 	ruff format --check .
# 	ruff check .
# 	MYPYPATH=src mypy --namespace-packages --explicit-package-bases --strict .

.PHONY: copyright-check apply-copyright fix-licenses check-licenses
## Copyright checks
copyright-check:
	docker run -it --rm -v $(CURDIR):/github/workspace apache/skywalking-eyes header check

## Add copyright notice to new files
apply-copyright:
	docker run -it --rm -v $(CURDIR):/github/workspace apache/skywalking-eyes header fix

fix-licenses: apply-copyright

check-licenses: copyright-check

.PHONY: build-linter-image lint-submodules check-linter-image check-dependencies lint-all mypy

IMAGE := drx-idp:latest
GIT_COMMIT := $(shell git log -1 --format=%H)

# Build linter image
build-linter-image:
	docker build -t $(IMAGE) -f docker/linter/Dockerfile .

check-dependencies:
	@echo "🎱 Checking dependencies..."
	pip install -r requirements.txt -r dev-requirements.txt -q
	@echo "🎱 Dependencies are installed"

check-linter-image:
	@echo "🎱 Checking if linter image exists..."
	@if DOCKER_CLI_EXPERIMENTAL=enabled docker manifest inspect $(IMAGE) > /dev/null; then \
		echo "🪕 image $(IMAGE) already exists. Build has been skipped "; \
	else \
		echo "🪇 image $(IMAGE) does not exist. Building..."; \
		make build-linter-image; \
	fi

# Run all linters
lint-all: check-linter-image
	docker run --rm -v $(PWD):/workspace -w /workspace $(IMAGE) ./linters/run_all.sh

lint-changed: check-linter-image
	sh ./linters/utils/diff.sh origin/main $(GIT_COMMIT) > changed-files.txt
	docker run -v $(PWD):/workspace -w /workspace $(IMAGE) sh -c "chmod +x ./linters/lint.sh && ./linters/lint.sh changed-files.txt"
	rm -rf changed-files.txt
# Run specific linter
lint-%: check-linter-image
	docker run --rm -v $(PWD):/workspace -w /workspace $(IMAGE) ./linters/lint_$*.sh

# Run specific linter with fix
fix-%: check-linter-image
	docker run --rm -v $(PWD):/workspace -w /workspace $(IMAGE) ./linters/lint_$*.sh "" --fix

mypy: check-dependencies
	MYPYPATH=src mypy --namespace-packages --explicit-package-bases --strict .
