import asyncio
import csv
import dataclasses
import datetime

from typing import TYPE_CHECKING, Any, AsyncIterator, Literal, Self, TypeAlias

from aiosalesforce.exceptions import SalesforceError
from aiosalesforce.utils import json_dumps, json_loads

if TYPE_CHECKING:
    from .client import BulkClientV2

QueryOperationType: TypeAlias = Literal["query", "queryAll"]

QueryJobState: TypeAlias = Literal[
    "UploadComplete",
    "InProgress",
    "JobComplete",
    "Aborted",
    "Failed",
]

ColumnDelimiter: TypeAlias = Literal[
    "BACKQUOTE",
    "CARET",
    "COMMA",
    "PIPE",
    "SEMICOLON",
    "TAB",
]

LineEnding: TypeAlias = Literal["LF", "CRLF"]


@dataclasses.dataclass
class QueryJobInfo:
    """Bulk API 2.0 query job information."""

    id: str
    operation: QueryOperationType
    object: str
    created_by_id: str
    created_date: datetime.datetime
    system_modstamp: datetime.datetime
    state: QueryJobState
    concurrency_mode: Literal["Parallel"]
    content_type: Literal["CSV"]
    api_version: str
    job_type: Literal["V2Query"] | None
    line_ending: LineEnding
    column_delimiter: ColumnDelimiter
    number_records_processed: int | None = None
    retries: int | None = None
    total_processing_time: int | None = None
    is_pk_chunking_supported: bool | None = None

    @classmethod
    def from_json(cls, data: bytes) -> Self:
        parsed_json = json_loads(data)
        job_info = cls(
            **{
                field.name: parsed_json.get(
                    "".join(
                        [
                            component.capitalize() if i > 0 else component
                            for i, component in enumerate(field.name.split("_"))
                        ]
                    ),
                    None,
                )
                for field in dataclasses.fields(cls)
            }
        )
        for attr in ["created_date", "system_modstamp"]:
            setattr(
                job_info,
                attr,
                datetime.datetime.fromisoformat(getattr(job_info, attr)),
            )
        return job_info


class BulkQueryClient:
    """
    Salesforce Bulk API 2.0 query client.

    This is a low-level client used to manage query jobs.

    Parameters
    ----------
    bulk_client : BulkClientV2
        Bulk API 2.0 client from this client is invoked.

    """

    bulk_client: "BulkClientV2"
    base_url: str
    """Base URL in the format https://[subdomain(s)].my.salesforce.com/services/data/v[version]/jobs/query"""

    def __init__(self, bulk_client: "BulkClientV2") -> None:
        self.bulk_client = bulk_client
        self.base_url = f"{self.bulk_client.base_url}/query"

    async def create_job(
        self,
        query: str,
        include_all_records: bool = False,
        column_delimiter: ColumnDelimiter = "COMMA",
        line_ending: LineEnding = "LF",
    ) -> QueryJobInfo:
        """
        Create a new query job.

        Parameters
        ----------
        query : str
            SOQL query to execute.
        include_all_records : bool, default False
            If True, executes queryAll instead of query.
        column_delimiter : ColumnDelimiter, default "COMMA"
            Column delimiter in result CSV.
        line_ending : LineEnding, default "LF"
            Line ending in result CSV.

        Returns
        -------
        QueryJobInfo
            Query job information.

        """
        payload: dict[str, str] = {
            "operation": "queryAll" if include_all_records else "query",
            "query": query,
            "contentType": "CSV",
            "columnDelimiter": column_delimiter,
            "lineEnding": line_ending,
        }
        response = await self.bulk_client.salesforce_client.request(
            "POST",
            self.base_url,
            content=json_dumps(payload),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        return QueryJobInfo.from_json(response.content)

    async def get_job(self, job_id: str) -> QueryJobInfo:
        """
        Get information about query job.

        Parameters
        ----------
        job_id : str
            Query job ID.

        Returns
        -------
        QueryJobInfo
            Job information.

        """
        response = await self.bulk_client.salesforce_client.request(
            "GET",
            f"{self.base_url}/{job_id}",
            headers={"Accept": "application/json"},
        )
        return QueryJobInfo.from_json(response.content)

    async def list_jobs(
        self,
        is_pk_chunking_enabled: bool | None = None,
        query_locator: str | None = None,
    ) -> AsyncIterator[QueryJobInfo]:
        """
        List all query jobs.

        Parameters
        ----------
        is_pk_chunking_enabled : bool | None, optional
            Filter by primary key chunking enabled, by default None.
        query_locator : str | None, optional
            Start listing from this query locator, by default None.

        Yields
        ------
        QueryJobInfo
            Job information.

        """
        params: dict[str, Any] = {}
        if is_pk_chunking_enabled is not None:
            params["isPkChunkingEnabled"] = is_pk_chunking_enabled
        if query_locator is not None:
            params["queryLocator"] = query_locator

        next_url: str | None = None
        while True:
            if next_url is None:
                response = await self.bulk_client.salesforce_client.request(
                    "GET",
                    self.base_url,
                    params=params or None,
                    headers={"Accept": "application/json"},
                )
            else:
                response = await self.bulk_client.salesforce_client.request(
                    "GET",
                    f"{self.bulk_client.salesforce_client.base_url}{next_url}",
                    headers={"Accept": "application/json"},
                )
            response_json: dict = json_loads(response.content)
            for record in response_json["records"]:
                yield QueryJobInfo.from_json(json_dumps(record))
            next_url = response_json.get("nextRecordsUrl", None)
            if next_url is None:
                break

    async def abort_job(self, job_id: str) -> QueryJobInfo:
        """
        Abort query job.

        Parameters
        ----------
        job_id : str
            Query job ID.

        Returns
        -------
        QueryJobInfo
            Job information.

        """
        response = await self.bulk_client.salesforce_client.request(
            "PATCH",
            f"{self.base_url}/{job_id}",
            content=json_dumps({"state": "Aborted"}),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        return QueryJobInfo.from_json(response.content)

    async def delete_job(self, job_id: str) -> None:
        """
        Delete query job.

        Parameters
        ----------
        job_id : str
            Query job ID.

        """
        await self.bulk_client.salesforce_client.request(
            "DELETE",
            f"{self.base_url}/{job_id}",
        )

    async def get_results(
        self,
        job_id: str,
        locator: str | None = None,
        max_records: int | None = None,
    ) -> tuple[list[dict[str, str]], str | None]:
        """
        Get one page of query results from a completed query job.

        Parameters
        ----------
        job_id : str
            Query job ID.
        locator : str | None, optional
            Result locator for the page, by default None.
        max_records : int | None, optional
            Maximum number of records to fetch for this page, by default None.

        Returns
        -------
        tuple[list[dict[str, str]], str | None]
            A tuple of (records, next_locator).
            next_locator is None when there are no more records.

        """
        params: dict[str, Any] = {}
        if locator is not None:
            params["locator"] = locator
        if max_records is not None:
            params["maxRecords"] = max_records

        response = await self.bulk_client.salesforce_client.request(
            "GET",
            f"{self.base_url}/{job_id}/results",
            params=params or None,
            headers={"Accept": "text/csv"},
        )
        reader = csv.DictReader(response.content.decode("utf-8").splitlines())
        records = list(reader)
        locator_header = response.headers.get("Sforce-Locator")
        next_locator = None if locator_header in {None, "null"} else locator_header
        return records, next_locator

    async def perform_query(
        self,
        query: str,
        include_all_records: bool = False,
        polling_interval: float = 5.0,
        max_records: int | None = None,
    ) -> AsyncIterator[dict[str, str]]:
        """
        Perform a Bulk API 2.0 query and stream all records.

        Parameters
        ----------
        query : str
            SOQL query to execute.
        include_all_records : bool, default False
            If True, executes queryAll.
        polling_interval : float, default 5.0
            Interval in seconds to poll query job status.
        max_records : int | None, default None
            Maximum number of records to request per result page.

        Yields
        ------
        dict[str, str]
            Query result record.

        """
        job = await self.create_job(
            query=query,
            include_all_records=include_all_records,
        )
        while job.state.lower().strip(" ") in {"uploadcomplete", "inprogress"}:
            await asyncio.sleep(polling_interval)
            job = await self.get_job(job.id)
        if job.state != "JobComplete":
            raise SalesforceError(
                (
                    f"Bulk API 2.0 query job failed with state '{job.state}'. "
                    f"Job ID: {job.id}"
                )
            )

        locator: str | None = None
        while True:
            records, locator = await self.get_results(
                job_id=job.id,
                locator=locator,
                max_records=max_records,
            )
            for record in records:
                yield record
            if locator is None:
                break
