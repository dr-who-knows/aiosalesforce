import dataclasses
import datetime

from unittest.mock import AsyncMock, patch

import httpx
import orjson
import pytest
import respx

from aiosalesforce.bulk.v2.client import BulkClientV2
from aiosalesforce.bulk.v2.query import BulkQueryClient, QueryJobInfo


def query_job_info_to_json(job: QueryJobInfo) -> bytes:
    job_dict = dataclasses.asdict(job)

    def to_camel_case(value: str) -> str:
        parts = "".join(part.capitalize() for part in value.split("_"))
        return parts[0].lower() + parts[1:]

    return orjson.dumps(
        {
            to_camel_case(field.name): job_dict[field.name]
            for field in dataclasses.fields(QueryJobInfo)
        }
    )


@pytest.fixture(scope="function")
def query_client(bulk_client: BulkClientV2) -> BulkQueryClient:
    return bulk_client.query_client


@pytest.fixture(scope="function")
def dummy_query_job(config: dict[str, str]) -> QueryJobInfo:
    return QueryJobInfo(
        id="7503h00000L0k2AAAR",
        operation="query",
        object="Account",
        created_by_id="00558000000yFyDAAU",
        created_date=datetime.datetime.now(),
        system_modstamp=datetime.datetime.now(),
        state="UploadComplete",
        concurrency_mode="Parallel",
        content_type="CSV",
        api_version=config["api_version"],
        job_type="V2Query",
        line_ending="LF",
        column_delimiter="COMMA",
        number_records_processed=0,
        retries=0,
        total_processing_time=0,
        is_pk_chunking_supported=True,
    )


async def test_create_job(
    httpx_mock_router: respx.MockRouter,
    query_client: BulkQueryClient,
    dummy_query_job: QueryJobInfo,
):
    async def side_effect(request: httpx.Request) -> httpx.Response:
        payload = orjson.loads(request.content)
        assert payload == {
            "operation": "queryAll",
            "query": "SELECT Id FROM Account",
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding": "LF",
        }
        return httpx.Response(
            status_code=200,
            content=query_job_info_to_json(dummy_query_job),
        )

    httpx_mock_router.post(query_client.base_url).mock(side_effect=side_effect)
    job = await query_client.create_job(
        query="SELECT Id FROM Account",
        include_all_records=True,
    )
    assert job == dummy_query_job


async def test_get_results(
    httpx_mock_router: respx.MockRouter,
    query_client: BulkQueryClient,
    dummy_query_job: QueryJobInfo,
):
    csv_payload = b'"Id","Name"\n"001","Acme"\n"002","Globex"\n'
    httpx_mock_router.get(f"{query_client.base_url}/{dummy_query_job.id}/results").mock(
        return_value=httpx.Response(
            status_code=200,
            content=csv_payload,
            headers={"Sforce-Locator": "abc123"},
        )
    )
    records, locator = await query_client.get_results(dummy_query_job.id)
    assert records == [{"Id": "001", "Name": "Acme"}, {"Id": "002", "Name": "Globex"}]
    assert locator == "abc123"


async def test_perform_query(
    httpx_mock_router: respx.MockRouter,
    bulk_client: BulkClientV2,
    query_client: BulkQueryClient,
    dummy_query_job: QueryJobInfo,
):
    job_id = dummy_query_job.id

    httpx_mock_router.post(query_client.base_url).mock(
        return_value=httpx.Response(
            status_code=200,
            content=query_job_info_to_json(dummy_query_job),
        )
    )

    def get_job_status(_: httpx.Request) -> httpx.Response:
        if dummy_query_job.state == "UploadComplete":
            dummy_query_job.state = "InProgress"
        elif dummy_query_job.state == "InProgress":
            dummy_query_job.state = "JobComplete"
        return httpx.Response(
            status_code=200,
            content=query_job_info_to_json(dummy_query_job),
        )

    httpx_mock_router.get(f"{query_client.base_url}/{job_id}").mock(
        side_effect=get_job_status
    )
    httpx_mock_router.get(f"{query_client.base_url}/{job_id}/results").mock(
        side_effect=[
            httpx.Response(
                status_code=200,
                content=b'"Id","Name"\n"001","Acme"\n',
                headers={"Sforce-Locator": "next-page"},
            ),
            httpx.Response(
                status_code=200,
                content=b'"Id","Name"\n"002","Globex"\n',
                headers={"Sforce-Locator": "null"},
            ),
        ]
    )

    sleep_mock = AsyncMock()
    with patch("asyncio.sleep", sleep_mock):
        records = []
        async for record in bulk_client.query(
            query="SELECT Id, Name FROM Account",
            max_records=50000,
        ):
            records.append(record)

    assert sleep_mock.await_count == 2
    assert records == [{"Id": "001", "Name": "Acme"}, {"Id": "002", "Name": "Globex"}]
