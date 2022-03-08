#!/bin/bash

docker run                                    \
    -ti --rm --network host                   \
    -e CLOUD_STORAGE="AZURE"                  \
    -e JOB_NAME="JOB_NAME_HERE"               \
    -e STORAGE_ACCOUNT_NAME="<placeholder>"   \
    -e STORAGE_ACCOUNT_KEY="<placeholder>"    \
    -e CONTAINER_NAME="<placeholder>"         \
    -e FILE_PREFIX="<placeholder>"            \
    -e FILE_NAME="<placeholder>"              \
    -e SINGLEPART="<placeholder>"             \
    -e CREDS_DIR="<placeholder>"              \
    -e SQL="`cat /your/sql/script/here.sql`"  \
    -e TERADATA_DATABASE="<placeholder>"      \
    -e TERADATA_USER="<placeholder>"          \
    -e TERADATA_PASSWORD="<placeholder>"      \
    -e FIELD_DELIMITER="|"                    \
    dro248/easy_tpt:latest
