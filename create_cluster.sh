#!/bin/bash
REGION=us-central1
CLUSTER_NAME=bigdataproj1
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --image-version=1.4 \
    --region=${REGION} \
    --metadata='CONDA_PACKAGES=scipy=1.1.0' \
    --metadata='PIP_PACKAGES=pandas==0.25.1 xgboost==1.1.1' \
    --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/python/conda-install.sh,gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh