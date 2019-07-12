PWD := $(shell pwd)
TAG ?= "minio/minsql"

all: build

build:
	@docker build -t $(TAG) .
	@docker create  --name minsql $(TAG)
	@docker cp minsql:/usr/bin/minsql .
	@docker rm minsql
