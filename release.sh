#!/bin/sh
set -eu

GO="/usr/bin/go"
GOARCH="$($GO env GOARCH)"

GHR="${GOPATH}/bin/ghr"
LATEST_TAG="$(git describe --abbrev=0 --tags)"

PKG="pg-statistics-to-es"
rm -f ./dist/${PKG}.zip

for i in $(find ./dist -mindepth 1 -type f ! -name '.zip')
do
    zip ./dist/${PKG}.zip $i
done

$GHR -replace ${LATEST_TAG} ./dist/${PKG}.zip
git push origin --tags
