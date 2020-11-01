#!/bin/bash
set -e

FUTSU_SECRET=${1}

rm -f secret.tar.gz
rm -rf secret.out

gpg --quiet --batch --yes --decrypt --passphrase="${FUTSU_SECRET}" --output secret.tar.gz secret.tar.gz.gpg
tar -xzf secret.tar.gz

rm -f secret.tar.gz
