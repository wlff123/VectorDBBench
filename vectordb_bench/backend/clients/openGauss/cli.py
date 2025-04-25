from typing import Annotated, Optional, TypedDict, Unpack

import click
import os
from pydantic import SecretStr

from vectordb_bench.backend.clients.api import MetricType

from ....cli.cli import (
    CommonTypedDict,
    HNSWFlavor1,
    IVFFlatTypedDict,
    cli,
    click_parameter_decorators_from_typed_dict,
    get_custom_case_config,
    run,
)
from vectordb_bench.backend.clients import DB

class openGaussTypedDict(CommonTypedDict):
    user_name: Annotated[
        str, click.option("--user-name", type=str, help="Db username", required=True)
    ]
    password: Annotated[
        str,
        click.option("--password",
                     type=str,
                     help="openGauss database password",
                     default=lambda: os.environ.get("POSTGRES_PASSWORD", ""),
                     show_default="$POSTGRES_PASSWORD",
                     ),
    ]

    host: Annotated[
        str, click.option("--host", type=str, help="Db host", required=True)
    ]
    port: Annotated[
        int, click.option("--port", type=int, help="Db port", required=True)
    ]
    db_name: Annotated[
        str, click.option("--db-name", type=str, help="Db name", required=True)
    ]
    maintenance_work_mem: Annotated[
        Optional[str],
        click.option(
            "--maintenance-work-mem",
            type=str,
            help="Sets the maximum memory to be used for maintenance operations (index creation). "
            "Can be entered as string with unit like '64GB' or as an integer number of KB."
            "This will set the parameters: max_parallel_maintenance_workers,"
            " max_parallel_workers & table(parallel_workers)",
            required=False,
        ),
    ]
    max_parallel_workers: Annotated[
        Optional[int],
        click.option(
            "--max-parallel-workers",
            type=int,
            help="Sets the maximum number of parallel processes per maintenance operation (index creation)",
            required=False,
        ),
    ]
    

class openGaussIVFFlatTypedDict(openGaussTypedDict, IVFFlatTypedDict):
    ...


@cli.command()
@click_parameter_decorators_from_typed_dict(openGaussIVFFlatTypedDict)
def openGaussIVFFlat(
    **parameters: Unpack[openGaussIVFFlatTypedDict],
):
    from .config import openGaussConfig, openGaussIVFFlatConfig

    parameters["custom_case"] = get_custom_case_config(parameters)
    run(
        db=DB.openGauss,
        db_config=openGaussConfig(
            db_label=parameters["db_label"],
            user_name=SecretStr(parameters["user_name"]),
            password=SecretStr(parameters["password"]),
            host=parameters["host"],
            port=parameters["port"],
            db_name=parameters["db_name"],
        ),
        db_case_config=openGaussIVFFlatConfig(
            metric_type=None,
            lists=parameters["lists"],
            probes=parameters["probes"]
        ),
        **parameters,
    )


class openGaussHNSWTypedDict(openGaussTypedDict, HNSWFlavor1):
    ...

@cli.command()
@click_parameter_decorators_from_typed_dict(openGaussHNSWTypedDict)
def openGaussHNSW(
    **parameters: Unpack[openGaussHNSWTypedDict],
):
    from .config import openGaussConfig, openGaussHNSWConfig

    run(
        db=DB.openGauss,
        db_config=openGaussConfig(
            db_label=parameters["db_label"],
            user_name=SecretStr(parameters["user_name"]),
            password=SecretStr(parameters["password"]),
            host=parameters["host"],
            port=parameters["port"],
            db_name=parameters["db_name"],
        ),
        db_case_config=openGaussHNSWConfig(
            m=parameters["m"],
            ef_construction=parameters["ef_construction"],
            ef_search=parameters["ef_search"],
            maintenance_work_mem=parameters["maintenance_work_mem"],
            max_parallel_workers=parameters["max_parallel_workers"],
        ),
        **parameters,
    )

class openGaussHNSWPQTypedDict(openGaussTypedDict, HNSWFlavor1):
    pq_m: Annotated[Optional[int], click.option("--pq_m", type=int, help="hnsw_pq_m")]
    pq_ksub: Annotated[Optional[int], click.option("--pq_ksub", type=int, help="hnsw_pq_ksub")]
    hnsw_earlystop_threshold: Annotated[Optional[int], click.option("--hnsw_earlystop_threshold", type=int, help="hnsw_earlystop_threshold")]

@cli.command()
@click_parameter_decorators_from_typed_dict(openGaussHNSWPQTypedDict)
def openGaussHNSWPQ(
    **parameters: Unpack[openGaussHNSWPQTypedDict],
):
    from .config import openGaussConfig, openGaussHNSWPQConfig

    run(
        db=DB.openGauss,
        db_config=openGaussConfig(
            db_label=parameters["db_label"],
            user_name=SecretStr(parameters["user_name"]),
            password=SecretStr(parameters["password"]),
            host=parameters["host"],
            port=parameters["port"],
            db_name=parameters["db_name"],
        ),
        db_case_config=openGaussHNSWPQConfig(
            m=parameters["m"],
            ef_construction=parameters["ef_construction"],
            ef_search=parameters["ef_search"],
            pq_m=parameters["pq_m"],
            hnsw_earlystop_threshold = parameters["hnsw_earlystop_threshold"],
            pq_ksub=parameters["pq_ksub"],
            maintenance_work_mem=parameters["maintenance_work_mem"],
            max_parallel_workers=parameters["max_parallel_workers"],
        ),
        **parameters,
    )
