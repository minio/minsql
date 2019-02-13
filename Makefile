PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)
LDFLAGS := $(shell go run buildscripts/gen-ldflags.go)

BUILD_LDFLAGS := '$(LDFLAGS)'

all: build

checks:
	@echo "Checking dependencies"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)
	@echo "Checking for project in GOPATH"
	@(env bash $(PWD)/buildscripts/checkgopath.sh)

getdeps:
	@echo "Installing golint" && go get -u golang.org/x/lint/golint
	@echo "Installing staticcheck" && go get -u honnef.co/go/tools/...
	@echo "Installing misspell" && go get -u github.com/client9/misspell/cmd/misspell

verifiers: getdeps vet fmt lint staticcheck spelling

vet:
	@echo "Running $@"
	@go vet github.com/minsql/minsql/...

fmt:
	@echo "Running $@"
	@gofmt -d cmd/
	@gofmt -d pkg/

lint:
	@echo "Running $@"
	@${GOPATH}/bin/golint -set_exit_status github.com/minsql/minsql/cmd/...
	@${GOPATH}/bin/golint -set_exit_status github.com/minsql/minsql/pkg/...

staticcheck:
	@echo "Running $@"
	@${GOPATH}/bin/staticcheck github.com/minsql/minsql/cmd/...
	@${GOPATH}/bin/staticcheck github.com/minsql/minsql/pkg/...

spelling:
	@${GOPATH}/bin/misspell -locale US -error `find server/`
	@${GOPATH}/bin/misspell -locale US -error `find docs/`
	@${GOPATH}/bin/misspell -locale US -error `find buildscripts/`

# Builds minsql, runs the verifiers then runs the tests.
check: test
test: verifiers build
	@echo "Running unit tests"
	@CGO_ENABLED=0 go test -tags kqueue ./...

coverage: build
	@echo "Running all coverage for minsql"
	@(env bash $(PWD)/buildscripts/go-coverage.sh)

# Builds minsql locally.
build: checks
	@echo "Building minsql binary to './minsql'"
	@GOFLAGS="" CGO_ENABLED=0 go build -tags kqueue --ldflags $(BUILD_LDFLAGS) -o $(PWD)/minsql

# Builds minsql and installs it to $GOPATH/bin.
install: build
	@echo "Installing minsql binary to '$(GOPATH)/bin/minsql'"
	@mkdir -p $(GOPATH)/bin && cp -u $(PWD)/minsql $(GOPATH)/bin/minsql
	@echo "Installation successful. To learn more, try \"minsql --help\"."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rvf minsql
	@rm -rvf build
	@rm -rvf release
