
CONTAINER_CLI ?= docker

HUB ?= release-ci.daocloud.io/baize

VERSION ?= dev-$(shell git rev-parse --short=8 HEAD)

BUILD_PLATFORM ?= linux/amd64
PUSH_PLATFORMS ?= linux/amd64,linux/arm64
MX_SMI_IMAGE ?= $(HUB)/mx-smi:v0.1

build-manager:
	$(CONTAINER_CLI) buildx build \
		-t $(HUB)/kcover-controller:$(VERSION) \
		-f docker/kcover.Dockerfile \
		--load \
		--platform $(BUILD_PLATFORM) \
		.

push-manager:
	$(CONTAINER_CLI) buildx build \
		-t $(HUB)/kcover-controller:$(VERSION) \
		-f docker/kcover.Dockerfile \
		--push \
		--platform $(PUSH_PLATFORMS) \
		.

build-mx-smi:
	$(CONTAINER_CLI) build \
		-t $(HUB)/mx-smi:v0.1 \
		-f docker/mx-smi.Dockerfile \
		--platform linux/amd64 \
		.

push-mx-smi: build-mx-smi
	$(CONTAINER_CLI) push $(HUB)/mx-smi:v0.1

image-mx-smi: push-mx-smi

build-agent:
	$(CONTAINER_CLI) buildx build \
		-t $(HUB)/kcover-agent:$(VERSION) \
		-f docker/agent.Dockerfile \
		--build-arg MX_SMI_IMAGE=$(MX_SMI_IMAGE) \
		--load \
		--platform $(BUILD_PLATFORM) \
		.

push-agent:
	$(CONTAINER_CLI) buildx build \
		-t $(HUB)/kcover-agent:$(VERSION) \
		-f docker/agent.Dockerfile \
		--build-arg MX_SMI_IMAGE=$(MX_SMI_IMAGE) \
		--push \
		--platform $(PUSH_PLATFORMS) \
		.

build: build-manager build-agent

push: push-manager push-mx-smi push-agent

image-agent: push-agent

image-manager: push-manager

image-controller: push-manager

images: push-manager push-mx-smi push-agent

test:
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

.PHONY: build build-agent build-manager build-mx-smi push push-agent push-manager push-mx-smi images image-mx-smi image-agent image-manager image-controller
