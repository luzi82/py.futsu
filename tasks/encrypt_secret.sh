#!/bin/bash
set -e

rm -f secret.tar.gz
rm -f secret.tar.gz.gpg

tar -czf secret.tar.gz secret
gpg --symmetric --cipher-algo AES256 --pinentry-mode loopback --passphrase-file secret/FUTSU_SECRET secret.tar.gz

rm -f secret.tar.gz
