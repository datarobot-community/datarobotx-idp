lint:
	ruff format .
	ruff check .
	MYPYPATH=src mypy --namespace-packages --explicit-package-bases --strict .
        
.PHONY: copyright-check apply-copyright fix-licenses check-licenses
## Copyright checks
copyright-check:
	docker run --rm -v $(CURDIR):/github/workspace \
	    ghcr.io/apache/skywalking-eyes/license-eye:785bb7f3810572d6912666b4f64bad28e4360799 \
	    -v info header check

## Add copyright notice to new files
apply-copyright:
	docker run --rm -v $(CURDIR):/github/workspace \
	    ghcr.io/apache/skywalking-eyes/license-eye:785bb7f3810572d6912666b4f64bad28e4360799 \
	    -v info header fix

fix-licenses: apply-copyright

check-licenses: copyright-check
