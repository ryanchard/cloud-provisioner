# A simple makefile to automate testing a bit

.PHONY: test apt-setup

testing-virtualenv: tests/requirements.txt
	virtualenv testing-virtualenv
	testing-virtualenv/bin/pip install -r tests/requirements.txt

test: testing-virtualenv
	testing-virtualenv/bin/nosetests -v --with-coverage --cover-package=ggprovisioner

# Requires sudo!
# the testing virtualenv depends upon this, but we don't want it to run every
# time, so it's not in the Make dependencies
apt-setup:
	apt-get update && apt-get install libpq-dev
