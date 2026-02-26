"""Build a dlt pipeline to ingest NYC taxi data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_source


@dlt.source
def taxi_rest_api_source():
    """Define dlt resources from NYC taxi data REST API endpoints."""
    # build a configuration dictionary for the REST API source
    # the API returns a top‑level JSON array of trip objects so we
    # instruct dlt to iterate over the array with a data_selector.
    # there is no `trip_id` field in the payload, therefore we remove
    # the incorrect primary_key (let dlt auto‑generate or infer one).
    return rest_api_source({
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net",
        },
        "resource_defaults": {
            # do not specify primary_key when it does not exist in the data
            "write_disposition": "append",
        },
        "resources": [
            {
                "name": "trips",
                "endpoint": {
                    "path": "/data_engineering_zoomcamp_api",
                    "method": "GET",
                    "data_selector": "$[*]",  # iterate root array elements
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        # first page number expected by the API
                        "page": 1,
                        # the API does *not* return a total count, so
                        # disable the default `total` lookup which
                        # otherwise throws an error
                        "total_path": None,
                        # stop automatically when an empty page arrives
                        "stop_after_empty_page": True,
                    },
                },
            },
        ],
    })


pipeline = dlt.pipeline(
    pipeline_name='taxi_pipeline',
    destination='duckdb',
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    # call the source function to obtain the config generator
    load_info = pipeline.run(taxi_rest_api_source())
  # noqa: T201
