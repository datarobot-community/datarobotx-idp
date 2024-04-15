lint:
	ruff format .
	ruff check .
	MYPYPATH=src mypy --namespace-packages --explicit-package-bases --strict .
        
