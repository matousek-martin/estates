.PHONY: lambda clean lint sync_data_from_s3 sync_data_to_s3

#################################################################################
# GLOBALS                                                                       #
#################################################################################

# Default
PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
PROJECT_NAME = estates

# S3
BUCKET = estates-9036941568
BRONZE_LAMBDA = estates-scraper
BRONZE_DIR = src/data/bronze
SILVER_LAMBDA = estates-silver-lambda
SILVER_DIR = src/data/silver

# Python
PANDAS_VERSION = pandas-1.1.5-cp38-cp38-manylinux1_x86_64.whl
NUMPY_VERSION = numpy-1.19.5-cp38-cp38-manylinux1_x86_64.whl
PANDAS_URL = https://files.pythonhosted.org/packages/f9/f4/ede7c643939c132b0692a737800747ce5ba0e8068af27730dfda936c9bf1/$(PANDAS_VERSION)
NUMPY_URL = https://files.pythonhosted.org/packages/21/da/4a59e01f8fff4281a068e90868edd62253c1431a1b7315fe6789f8a0d9c0/$(NUMPY_VERSION)

ifeq (,$(shell which conda))
HAS_CONDA=False
else
HAS_CONDA=True
endif

#################################################################################
# COMMANDS                                                                      #
#################################################################################
## Deploy Bronze layer Lambda (scraper) to AWS
bronze:
ifeq ($(travis), true)
	pip3 install --system boto3 requests -t ./$(BRONZE_DIR)
else
	pip3 install boto3 requests -t ./$(BRONZE_DIR)
endif
	cd $(BRONZE_DIR); zip -r bronze.zip *; cd $(PROJECT_DIR)
	aws lambda update-function-code --function-name $(BRONZE_LAMBDA) --zip-file fileb://$(BRONZE_DIR)/bronze.zip
ifeq ($(travis), )
	find . \! -name 'scraper.py' \! -name 'lambda_function.py' -path '*$(BRONZE_DIR)*' -delete
endif

## Deploy Silver layer Lambda to AWS
silver:
ifeq ($(travis), true)
	pip3 install --system -r $(SILVER_DIR)/requirements.txt -t ./$(SILVER_DIR)
else
	pip3 install -r $(SILVER_DIR)/requirements.txt -t ./$(SILVER_DIR)
endif
	#rm -r $(SILVER_DIR)/pandas $(SILVER_DIR)/numpy $(SILVER_DIR)/*.dist-info
	wget -O $(SILVER_DIR)/$(PANDAS_VERSION) $(PANDAS_URL)
	wget -O $(SILVER_DIR)/$(NUMPY_VERSION) $(NUMPY_URL)
	unzip $(SILVER_DIR)/$(PANDAS_VERSION) -d $(SILVER_DIR)
	unzip $(SILVER_DIR)/$(NUMPY_VERSION) -d $(SILVER_DIR)
	cd $(SILVER_DIR); rm -r *.whl *.dist-info __pycache__; zip -r silver.zip *; cd $(PROJECT_DIR)
	aws s3 cp $(SILVER_DIR)/silver.zip s3://$(BUCKET)/lambda/silver.zip
	aws lambda update-function-code --function-name $(SILVER_LAMBDA) --s3-bucket $(BUCKET) --s3-key lambda/silver.zip
ifeq ($(travis), )
	find . \! -name 'prepare_data.py' \! -name 'lambda_function.py' \! -name 'requirements.txt' -path '*$(SILVER_DIR)*' -delete
endif

## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

## Lint using pylint
lint:
	pylint src

## Upload Data to S3
sync_data_to_s3:
	aws s3 sync data/ s3://$(BUCKET)/data/

## Download Data from S3
sync_data_from_s3:
	aws s3 sync s3://$(BUCKET)/data/ data/ --exclude 'bronze/*'
