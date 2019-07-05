#!/bin/sh

GOARCH := $(shell $(GO) env GOARCH)

GHR ?= $(GOPATH)/bin/ghr
LATEST_TAG="$(git describe --abbrev=0 --tags)"

PKG="pg-statistics-to-es"

for i in $(find ./dist -mindepth 1 -type f ! -name '.zip')
do
    zip ./dist/${PKG}.zip $i
done

$GHR -replace ${LATEST_TAG} ./dist/${PKG}.zip
