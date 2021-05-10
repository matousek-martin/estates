.PHONY: lambda clean lint sync_data_from_s3 sync_data_to_s3

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
PROJECT_NAME = estates
LAMBDA_FUNCTION = estates-scraper
BUCKET = estates-9036941568

ifeq (,$(shell which conda))
HAS_CONDA=False
else
HAS_CONDA=True
endif

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Deploy Lambda to AWS and clean up
lambda:
	cd src
	pip3 install --system boto3 requests -t ./scrapers
	cd scrapers; zip -r lambda.zip *; cd ..
	aws lambda update-function-code --function-name $(LAMBDA_FUNCTION) --zip-file fileb://scrapers/lambda.zip
	find . \! -name 'scraper.py' \! -name 'lambda_function.py' -path '*scrapers/*' -delete

## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

## Lint using pylint
lint:
	pylint src

## Upload Data to S3
sync_data_to_s3:
	aws s3 sync data/ s3://$(BUCKET)/

## Download Data from S3
sync_data_from_s3:
	aws s3 sync s3://$(BUCKET)/ data/
