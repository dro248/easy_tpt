#!/bin/bash

docker run                                    \
    -ti --rm --network host                   \
    -e CLOUD_STORAGE="S3"                     \
    -e JOB_NAME="JOB_NAME_HERE"               \
    -e AWS_ACCESS_KEY_ID="<placeholder>"      \
    -e AWS_SECRET_ACCESS_KEY="<placeholder>"  \
    -e S3_BUCKET="<placeholder>"              \
    -e S3_REGION="<placeholder>"              \
    -e S3_FILEPATH="<placeholder>"            \
    -e S3_FILENAME="<placeholder>"            \
    -e S3_DONT_SPLIT_ROWS="True"              \
    -e WRITE_TO_SINGLE_FILE="False"           \
    -e SQL="`cat /your/sql/script/here.sql`"  \
    -e TERADATA_DATABASE="<placeholder>"      \
    -e TERADATA_USER="<placeholder>"          \
    -e TERADATA_PASSWORD="<placeholder>"      \
    -e FIELD_DELIMITER="|"                    \
    dro248/easy_tpt:latest
