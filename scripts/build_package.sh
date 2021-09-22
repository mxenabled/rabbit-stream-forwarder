#!/bin/sh
############ build_package.sh ############
# Builds a debian package using the fpm tool
# This script is intended to be used by gitlab-ci 
# It may also be executed manually for debugging
# 
# GOPRIVATE="*.mx.com" CGO_ENABLED=0 go build -o rabbit-stream-forwarder .
# docker image pull gitlab.mx.com:4567/mx/ci-images/fpm:latest
# docker run -it -v $(pwd):/data --workdir /data -e VERSION=0.0.1 fpm scripts/build_package.sh
# docker run -it -v $(pwd):/data --workdir /data ubuntu:latest dpkg -c output/rabbit-stream-forwarder_0.0.1-1_amd64.deb

set -v 
if [ -z ${VERSION+x} ]; then echo "VERSION is unset"; exit 1; fi
if [ -z ${ARCH+x} ]; then echo "ARCH is unset. Defaulting to 'amd64'"; export ARCH='amd64' ; fi

rm -r output/ || true
rm -r buildroot/ || true

mkdir output 
mkdir buildroot
mkdir -p buildroot/usr/local/bin
cp rabbit-stream-forwarder buildroot/usr/local/bin
cp ./rabbit-stream-forwarder.postinst build/root/rabbit-stream-forwarder.postinst
if [ ! -f buildroot/usr/local/bin/rabbit-stream-forwarder ]; then echo "binary missing"; exit 1; fi
chmod -R 0755 buildroot

# Generate the systemd unit files
pleaserun \
--name rabbit-stream-forwarder \
--platform systemd \
--user rabbit-stream-forwarder \
--group rabbit-stream-forwarder \
--description "Rabbit Stream Forwarder" \
--install-prefix ./buildroot/ \
--no-install-actions \
/usr/local/bin/rabbit-stream-forwarder

# Workaround for bug in pleaserun
# https://github.com/jordansissel/pleaserun/issues/147
rm buildroot/install_actions.sh

# pleaserun doesn't support adding multiple env files for the systemd service script
# this is so we can support that ability
rm ./buildroot/etc/systemd/system/rabbit-stream-forwarder.service
cp ./scripts/rabbit-stream-forwarder.service ./buildroot/etc/systemd/system/rabbit-stream-forwarder.service

# Generate the .deb package
fpm \
--input-type dir \
--output-type deb \
--url "https://gitlab.mx.com/mx/rabbit-stream-forwarder" \
--license Proprietary \
--chdir ./buildroot \
--name rabbit-stream-forwarder \
--version "${VERSION}" \
--description "Rabbit Stream Forwarder" \
--maintainer "MX-Platform Stability" \
--architecture "${ARCH}" \
--after-install ./scripts/rabbit-stream-forwarder.postinst \
--provides rabbit-stream-forwarder \
--package output/ .
