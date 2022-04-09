#!/bin/bash
echo "Login with Google Cloud Registry to push the images."
echo "You should see 'Login Success' if things go well."
security -v unlock-keychain ~/Library/Keychains/login.keychain-db
gcloud auth print-access-token | \
    docker login -u oauth2accesstoken \
    --password-stdin https://gcr.io