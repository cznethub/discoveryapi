

.PHONY: build
build:
	docker build -t discoveryapi .
	gcloud builds submit --tag gcr.io/iguide-375219/discoveryapi