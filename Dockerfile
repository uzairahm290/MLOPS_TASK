FROM astrocrpublic.azurecr.io/runtime:3.3-12

LABEL maintainer="Uzair Ahmad <uzairahm290@gmail.com>" \
      org.opencontainers.image.title="currency-etl-airflow" \
      org.opencontainers.image.description="Astro Airflow runtime for the Frankfurter -> MongoDB currency ETL" \
      org.opencontainers.image.source="https://github.com/uzairahm290/MLOPS_TASK" \
      com.mlops.project="currency-etl" \
      com.mlops.network="currency_etl_network" \
      com.mlops.storage="MongoDB volume currency_mongo_data"
