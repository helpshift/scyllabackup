# created in the ./.env directory and will be setup with all deps for
# running the tests.
clean:
	find . -name '*.pyc' -delete

test:
	tox -r

setup-repl:
	tox -r --notest
	.tox/py27/bin/ipython

repl:
	.tox/py27/bin/ipython
