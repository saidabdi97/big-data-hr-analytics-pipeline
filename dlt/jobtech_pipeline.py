import dlt
import requests

URL = "https://jobsearch.api.jobtechdev.se/search"


@dlt.resource(
    write_disposition="replace",
    columns={
        "original_id": {"data_type": "text"},
        "description__company_information": {"data_type": "text"},
        "description__needs": {"data_type": "text"},
        "description__requirements": {"data_type": "text"},
        "access": {"data_type": "text"},
        "employer__phone_number": {"data_type": "text"},
        "employer__email": {"data_type": "text"},
        "application_details__other": {"data_type": "text"},
        "removed_date": {"data_type": "text"},
    }
)
def jobtech_ads():
    params = {"q": "data", "limit": 100}
    headers = {"User-Agent": "big-data-hr"}

    r = requests.get(URL, params=params, headers=headers)
    r.raise_for_status()

    for ad in r.json().get("hits", []):
        yield ad


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="jobtech",
        destination="duckdb",
        dataset_name="raw"
    )

    pipeline.run(jobtech_ads())
