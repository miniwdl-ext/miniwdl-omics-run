import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import ExitStack

import boto3
import botocore.config
import WDL
import WDL.CLI
from WDL._util import configure_logger

from ._version import __version__


def main(argv=sys.argv):
    # parse CLI arguments
    args = arg_parser().parse_args(argv[1:])

    # set up logger
    logging.basicConfig(level=(logging.DEBUG if args.debug else logging.INFO))
    with ExitStack() as cleanup:
        cleanup.enter_context(configure_logger())
        logger = logging.getLogger("miniwdl-omics-run")

        # load WDL document
        wdl_doc = WDL.load(
            args.uri,
            args.path or [],
            read_source=WDL.CLI.make_read_source(False),
        )
        if not wdl_doc.workflow and len(wdl_doc.tasks) != 1:
            logger.error("main WDL file must have a workflow or a single task")
            sys.exit(1)

        # parse & validate the inputs
        if not args.build:
            if not args.output_uri:
                logger.error("--output-uri URI is required to start run")
                sys.exit(1)
            try:
                wdl_exe, input_env, _ = WDL.CLI.runner_input(
                    wdl_doc,
                    args.inputs,
                    args.input_file,
                    args.empty,
                    args.none,
                    downloadable=check_s3_uri_input,
                )
            except WDL.Error.InputError as exn:
                logger.error(exn.args[0])
                sys.exit(1)
            input_dict = WDL.values_to_json(input_env)
            logger.debug("run inputs = " + json.dumps(input_dict))
        else:  # args.build
            if (
                args.inputs
                or args.input_file
                or args.empty
                or args.none
                or args.output_uri
            ):
                logger.error(
                    "workflow input/output arguments are not applicable with --build"
                )
                sys.exit(1)
            wdl_exe = wdl_doc.workflow or wdl_doc.tasks[0]
        logger.debug(
            f"WDL={os.path.basename(wdl_doc.pos.abspath)}"
            f" exe={wdl_exe.name} digest={wdl_exe.digest}"
        )

        # TODO: scan all task runtime.docker and complain if they aren't on ECR

        # get/create Omics workflow
        omics = boto3.client(
            "omics", config=botocore.config.Config(retries={"mode": "standard"})
        )
        workflow_id = ensure_omics_workflow(logger, cleanup, omics, wdl_doc, wdl_exe)
        await_omics_workflow(logger, omics, workflow_id)

        if args.build:
            print(json.dumps({"workflowId": workflow_id}, indent=2))
            sys.exit(0)

        # start run
        res = omics.start_run(
            outputUri=args.output_uri,
            parameters=input_dict,
            roleArn=args.role_arn,
            workflowId=workflow_id,
            workflowType="PRIVATE",
            logLevel="ALL",
            requestId=str(uuid.uuid4()),
            **start_run_options(args),
        )

    run_id = res["id"]
    aws_region = omics.meta.region_name
    run_info = {
        "workflowId": workflow_id,
        "runId": run_id,
        "runConsole": f"https://{aws_region}.console.aws.amazon.com/omics/home"
        f"?region={aws_region}#/runs/{run_id}",
    }
    print(json.dumps(run_info, indent=2))


def arg_parser():
    parser = argparse.ArgumentParser("miniwdl-omics-run")
    parser.add_argument(
        "--version",
        action=VersionAction,
        help="show package version information",
    )
    parser.add_argument(
        "--debug", action="store_true", help="maximally verbose logging"
    )

    group = parser.add_argument_group("WDL source")
    group.add_argument(
        "uri", metavar="MAIN_WDL", type=str, help="WDL document filename/URI"
    )
    group.add_argument(
        "-p",
        "--path",
        metavar="DIR",
        type=str,
        action="append",
        help="local directory to search for imports (can supply multiple times)",
    )
    group.add_argument(
        "-b", "--build", action="store_true", help="build workflow only (do not run)"
    )

    group = parser.add_argument_group("run inputs")
    group.add_argument(
        "inputs",
        metavar="input_key=value",
        type=str,
        nargs="*",
        help="Workflow inputs. Optional space between = and value."
        " For arrays repeat, key=value1 key=value2 ...",
    )
    group.add_argument(
        "-i",
        "--input",
        metavar="INPUT.json",
        dest="input_file",
        help="Cromwell-style input JSON object, filename, or -"
        "; command-line inputs will be merged in",
    )
    group.add_argument(
        "--empty",
        metavar="input_key",
        action="append",
        help="explicitly set a string input to the empty string"
        " OR an array input to the empty array",
    )
    group.add_argument(
        "--none",
        metavar="input_key",
        action="append",
        help="explicitly set an optional input to None (to override a default)",
    )

    group = parser.add_argument_group("Omics run configuration")
    group.add_argument(
        "--role-arn",
        metavar="ARN",
        type=str,
        help="ARN of IAM role",
        required=True,
    )
    group.add_argument(
        "--output-uri",
        metavar="OUTPUT_S3_URI",
        type=check_s3_uri_arg,
        help="S3 URI prefix for workflow outputs",
    )
    group.add_argument("--name", type=str, help="Name", default=None)
    group.add_argument("--priority", type=int, help="Priority (integer)", default=None)
    group.add_argument("--run-group-id", type=str, help="Run group ID", default=None)
    group.add_argument(
        "--storage-capacity",
        type=int,
        help="Storage capacity in gigabytes",
        default=None,
    )

    return parser


def start_run_options(args):
    ans = {}
    if args.name is not None:
        ans["name"] = args.name
    if args.priority is not None:
        ans["priority"] = args.priority
    if args.run_group_id is not None:
        ans["runGroupId"] = args.run_group_id
    if args.storage_capacity is not None:
        ans["storageCapacity"] = args.storage_capacity
    return ans


class VersionAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        super().__init__(option_strings, dest, nargs=0, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        print(f"miniwdl-omics-run v{__version__}")
        subprocess.call(["miniwdl", "--version"])
        parser.exit()


def is_s3_uri(x):
    # TODO: allow omics://
    return isinstance(x, str) and x.startswith("s3://")


def check_s3_uri_arg(x):
    if not is_s3_uri(x):
        raise argparse.ArgumentTypeError("OUTPUT_S3_URI must be a s3:// URI")
    return x


def check_s3_uri_input(path, _is_directory):
    if not is_s3_uri(path):
        raise WDL.Error.InputError("File/Directory input is not a s3:// URI: " + path)
    return path


def ensure_omics_workflow(logger, cleanup, omics, wdl_doc, wdl_exe):
    """
    Get an Omics workflow id suitable for running the given WDL -- reusing an existing
    one if found, otherwise creating it.
    """

    # Embed a content digest of the WDL source code into the workflow name. We assume
    # 16 characters of the digest is practically sufficient.
    omics_workflow_name = wdl_exe.name[:111] + "." + wdl_exe.digest[:16]

    # Look for an existing workflow with this name
    existing_count = 0
    existing_id = None
    for page in omics.get_paginator("list_workflows").paginate(
        name=omics_workflow_name, type="PRIVATE"
    ):
        for existing in page["items"]:
            if existing["status"] in ("DELETED", "FAILED"):
                continue
            existing_count += 1
            existing_id = existing["id"]

    if existing_id is not None:
        if existing_count > 1:
            logger.warning(
                f"multiple existing Omics workflows named {omics_workflow_name}"
                f"; using arbitrary one ({existing_id})"
            )
        else:
            logger.info(
                f"using existing Omics workflow id={existing_id} name="
                + omics_workflow_name
            )
        return existing_id

    # Otherwise, create one
    return create_omics_workflow(
        logger, cleanup, omics, omics_workflow_name, wdl_doc, wdl_exe
    )


def create_omics_workflow(logger, cleanup, omics, workflow_name, wdl_doc, wdl_exe):
    """
    Create a new Omics workflow for this WDL
    """

    # zip up the source code
    wdl_zip = zip_wdl(logger, cleanup, wdl_doc)

    # formulate the Omics parameter template based on the WDL inputs
    parameter_template = {}
    for b in wdl_exe.available_inputs:
        parameter_template[b.name] = {
            "description": b.name,  # TODO: get from parameter_meta
            "optional": b.name not in wdl_exe.required_inputs,
        }

    # create workflow
    logger.debug(
        f"creating Omics workflow {workflow_name} with parameter template: "
        + json.dumps(parameter_template)
    )
    res = omics.create_workflow(
        definitionZip=wdl_zip,
        engine="WDL",
        main=os.path.basename(wdl_doc.pos.abspath),
        name=workflow_name,
        parameterTemplate=parameter_template,
    )
    workflow_id = res["id"]
    logger.info(f"created Omics workflow id={workflow_id} name={workflow_name}")

    return workflow_id


def zip_wdl(logger, cleanup, wdl_doc):
    """
    Zip up the WDL source code (along with any other WDL files it imports)
    """
    logger = logger.getChild("zip")
    tmp_zip = cleanup.enter_context(
        tempfile.NamedTemporaryFile(
            prefix=os.path.basename(wdl_doc.pos.abspath) + ".", suffix=".zip"
        )
    )
    WDL.Zip.build(wdl_doc, tmp_zip.name, logger)
    with open(tmp_zip.name, "rb") as infile:
        zip_data = infile.read()
    logger.debug(f"zipped {wdl_doc.pos.uri} to {tmp_zip.name} ({len(zip_data)} bytes)")
    return zip_data


def await_omics_workflow(logger, omics, workflow_id):
    """
    Wait for Omics workflow to finish CREATING
    """
    while True:
        workflow_details = omics.get_workflow(export=[], id=workflow_id, type="PRIVATE")
        status = workflow_details["status"]
        msg = f"Omics workflow {workflow_id} status {status}, " + workflow_details.get(
            "statusMessage", "(no status message)"
        )
        if status == "FAILED":
            logger.error(msg)
            sys.exit(2)
        logger.debug(msg)
        if status != "CREATING":
            break
        time.sleep(1)
    logger.debug("workflow details: " + str(workflow_details))


if __name__ == "__main__":
    sys.exit(main())
