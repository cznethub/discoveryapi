
.PHONY: build
build:
	docker build -t discoveryapi .
	gcloud builds submit --tag gcr.io/iguide-375219/discoveryapi

.PHONY: run
run:
	docker build -t discoveryapi .
	docker run -p 8080:8080 discoveryapi
