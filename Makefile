
CONTAINER_CLI ?= docker

HUB ?= release-ci.daocloud.io/baize

VERSION ?= dev-$(shell git rev-parse --short=8 HEAD)

image-%:
	 $(CONTAINER_CLI) buildx build \
 		-t $(HUB)/kcover-$*:$(VERSION) \
 		-f docker/$*.Dockerfile \
 		--push \
 		--platform linux/amd64,linux/arm64 \
 		.

image-mx-smi:
	$(CONTAINER_CLI) buildx build \
		-t $(HUB)/mx-smi:v0.1 \
		-f docker/mx-smi.Dockerfile \
		--push \
		--platform linux/amd64 \
		.

image-agent:
	$(CONTAINER_CLI) buildx build \
		-t $(HUB)/kcover-agent:$(VERSION) \
		-f docker/agent.Dockerfile \
		--build-arg MX_SMI_IMAGE=$(HUB)/mx-smi:v0.1 \
		--push \
		--platform linux/amd64 \
		.

images: image-controller image-mx-smi image-agent

test:
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

.PHONY: images image-mx-smi image-agent
