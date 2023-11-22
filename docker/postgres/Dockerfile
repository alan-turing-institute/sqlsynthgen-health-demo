FROM postgres:alpine
COPY indian_liver_patient.csv /tmp/indian_liver_patient.csv
COPY initdb.sql /docker-entrypoint-initdb.d/initdb.sql
COPY ./create_replica.sh /docker-entrypoint-initdb.d/1_create_replica.sh