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
import botocore.exceptions
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
            if not (args.role or args.role_arn):
                logger.error("one of --role or --role-arn is required to start run")
                sys.exit(1)
            if args.role:
                if args.role_arn:
                    logger.error("supply only one of --role or --role-arn")
                    sys.exit(1)
                args.role_arn = resolve_iam_role_arn(logger, args.role)
            try:
                wdl_exe, input_env, _ = WDL.CLI.runner_input(
                    wdl_doc,
                    args.inputs,
                    args.input_file,
                    args.empty,
                    args.none,
                    downloadable=check_uri_input,
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
        if args.run_group:
            args.run_group_id = resolve_run_group_id(logger, omics, args.run_group)

        workflow_version_name = None
        if args.legacy_workflow_name:
            workflow_id = ensure_omics_workflow_legacy(
                logger, cleanup, omics, wdl_doc, wdl_exe
            )
        else:
            # New versioned workflow behavior
            workflow_id, workflow_version_name = ensure_omics_workflow_and_version(
                logger, cleanup, omics, wdl_doc, wdl_exe
            )

        if args.build:
            print(json.dumps({"workflowId": workflow_id}, indent=2))
            sys.exit(0)

        # resolve run cache if requested
        if args.cache or args.cache_id:
            args.cache_id = resolve_cache_id(logger, omics, args.cache, args.cache_id)
        elif args.cache_behavior is not None:
            logger.error("must supply --cache or --cache-id to use --cache-behavior")
            sys.exit(1)

        # start run
        start_kwargs = dict(
            outputUri=args.output_uri,
            parameters=input_dict,
            roleArn=args.role_arn,
            workflowId=workflow_id,
            workflowType="PRIVATE",
            logLevel="ALL",
            requestId=str(uuid.uuid4()),
            **start_run_options(args),
        )
        if workflow_version_name:
            start_kwargs["workflowVersionName"] = workflow_version_name
        res = omics.start_run(**start_kwargs)

    run_id = res["id"]
    aws_region = omics.meta.region_name
    run_info = {
        "workflowId": workflow_id,
        "workflowVersionName": workflow_version_name,
        "runId": run_id,
        "runConsole": f"https://{aws_region}.console.aws.amazon.com/omics/home"
        f"?region={aws_region}#/runs/{run_id}",
    }

    print(json.dumps(run_info, indent=2))


_CACHE_BEHAVIOR_MAP = {
    "no": "NO_CACHE",
    "failure": "CACHE_ON_FAILURE",
    "always": "CACHE_ALWAYS",
}
for k, v in list(_CACHE_BEHAVIOR_MAP.items()):
    _CACHE_BEHAVIOR_MAP[v] = v


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
    group.add_argument("--role", type=str, help="Name of IAM role")
    group.add_argument(
        "--role-arn",
        metavar="ARN",
        type=str,
        help="ARN of IAM role (saves API call to resolve --role)",
    )
    group.add_argument(
        "--output-uri",
        metavar="OUTPUT_S3_URI",
        type=check_s3_uri_arg,
        help="S3 URI prefix for workflow outputs",
    )
    group.add_argument("--name", type=str, help="Run name", default=None)
    group.add_argument("--priority", type=int, help="Priority (integer)", default=None)
    run_group = group.add_mutually_exclusive_group(required=False)
    run_group.add_argument(
        "--run-group",
        type=str,
        help="Run group name",
        default=None,
    )
    run_group.add_argument(
        "--run-group-id",
        type=str,
        help="Run group ID (saves API call to resolve --run-group)",
        default=None,
    )
    group.add_argument(
        "--storage-capacity",
        type=int,
        help="Static run storage capacity, in gigabytes",
        default=None,
    )
    group.add_argument(
        "--storage-type",
        type=str,
        choices=["static", "STATIC", "dynamic", "DYNAMIC"],
        help="Run storage type",
        default=None,
    )
    group.add_argument(
        "--retention-mode",
        type=str,
        choices=["retain", "RETAIN", "remove", "REMOVE"],
        help="Run retention mode",
        default=None,
    )

    cache_group = group.add_mutually_exclusive_group(required=False)
    cache_group.add_argument("--cache", help="Cache name", type=str)
    cache_group.add_argument(
        "--cache-id",
        type=str,
        help="Cache ID (saves API call to resolve --cache)",
    )
    group.add_argument(
        "--cache-behavior",
        choices=_CACHE_BEHAVIOR_MAP.keys(),
        default=None,
        help="Cache behavior override",
    )

    group.add_argument(
        "--legacy-workflow-name",
        action="store_true",
        help=(
            "Use legacy workflow naming (no workflow versioning; each version is a "
            "separate Omics workflow named with content digest suffix)."
        ),
    )

    return parser


def start_run_options(args):
    mappings = [
        ("name", "name", None),
        ("priority", "priority", None),
        ("run_group_id", "runGroupId", None),
        ("storage_capacity", "storageCapacity", None),
        ("storage_type", "storageType", lambda v: v.upper()),
        ("cache_id", "cacheId", None),
        ("cache_behavior", "cacheBehavior", lambda v: _CACHE_BEHAVIOR_MAP[v]),
        ("retention_mode", "retentionMode", lambda v: v.upper()),
    ]
    ans = {}
    for attr, key, transform in mappings:
        val = getattr(args, attr)
        if val is not None:
            ans[key] = transform(val) if transform else val
    return ans


class VersionAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        super().__init__(option_strings, dest, nargs=0, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        print(f"miniwdl-omics-run v{__version__}")
        subprocess.call(["miniwdl", "--version"])
        parser.exit()


def is_s3_uri(x):
    return isinstance(x, str) and x.startswith("s3://")


def is_omics_uri(x):
    return isinstance(x, str) and x.startswith("omics://")


def check_s3_uri_arg(x):
    if not is_s3_uri(x):
        raise argparse.ArgumentTypeError("OUTPUT_S3_URI must be a s3:// URI")
    return x


def check_uri_input(path, _is_directory):
    if not (is_s3_uri(path) or is_omics_uri(path)):
        raise WDL.Error.InputError(
            "File/Directory input is not a s3:// nor an omics:// URI: " + path
        )
    return path


def ensure_omics_workflow_legacy(logger, cleanup, omics, wdl_doc, wdl_exe):
    """
    Legacy behavior: derive workflow name by embedding a short WDL content digest into
    the name and do not use workflow versions/tags. Retained for backward compatibility.
    Ensures the workflow exists and is ready (not CREATING) before returning its id.
    """

    # Embed a content digest of the WDL source code into the workflow name. We assume
    # 16 characters of the digest is practically sufficient.
    omics_workflow_name = wdl_exe.name[:111] + "." + wdl_exe.digest[:16]

    workflow_id = select_existing_workflow_id(logger, omics, omics_workflow_name)
    if workflow_id:
        logger.info(
            f"using existing Omics workflow id={workflow_id} name={omics_workflow_name}"
        )
    else:
        workflow_id = create_omics_workflow(
            logger, cleanup, omics, omics_workflow_name, wdl_doc, wdl_exe
        )

    # Wait for workflow to finish creating
    await_omics_workflow(logger, omics, workflow_id)
    return workflow_id


def ensure_omics_workflow_and_version(logger, cleanup, omics, wdl_doc, wdl_exe):
    """
    Ensure a base Omics workflow named exactly as the WDL workflow name
    (tagged for this tool), and ensure a workflow version named by the
    WDL content digest. Returns (workflow_id, version_name).
    """
    TAG_KEY = "miniwdl-omics-run"
    TAG_VAL = "yes"

    # Base workflow name matches WDL workflow name exactly
    base_name = wdl_exe.name[:128]

    # Try to find an existing base workflow with our tag
    workflow_id = select_existing_workflow_id(
        logger,
        omics,
        base_name,
        require_tag=(TAG_KEY, TAG_VAL),
    )

    wdl_zip = None

    if not workflow_id:
        wdl_zip = zip_wdl(logger, cleanup, wdl_doc)
        workflow_id = create_omics_workflow(
            logger,
            cleanup,
            omics,
            base_name,
            wdl_doc,
            wdl_exe,
            tags={TAG_KEY: TAG_VAL},
            definition_zip=wdl_zip,
        )

    # Ensure base workflow is ready before creating/using versions
    await_omics_workflow(logger, omics, workflow_id)

    # Ensure version
    version_name = wdl_exe.digest[:16]

    existing_version = None
    try:
        existing_version = omics.get_workflow_version(
            workflowId=workflow_id, versionName=version_name, type="PRIVATE"
        )
        logger.info(
            "using existing Omics workflow id=%s name=%s version=%s",
            workflow_id,
            base_name,
            version_name,
        )
    except botocore.exceptions.ClientError as ce:
        code = ce.response.get("Error", {}).get("Code", "")
        status = ce.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        not_found = (
            code in ("NotFoundException", "ResourceNotFoundException") or status == 404
        )
        if not not_found:
            raise

    if not existing_version:
        if wdl_zip is None:
            wdl_zip = zip_wdl(logger, cleanup, wdl_doc)
        parameter_template = parameter_template_from_wdl(wdl_exe)
        omics.create_workflow_version(
            workflowId=workflow_id,
            versionName=version_name,
            definitionZip=wdl_zip,
            engine="WDL",
            main=os.path.basename(wdl_doc.pos.abspath),
            parameterTemplate=parameter_template,
            tags={TAG_KEY: TAG_VAL},
        )
        logger.info(
            "created Omics workflow id=%s name=%s version=%s",
            workflow_id,
            base_name,
            version_name,
        )

    if not existing_version or existing_version.get("status") == "CREATING":
        # Wait for version to finish creating
        await_omics_entity(
            logger,
            lambda: omics.get_workflow_version(
                workflowId=workflow_id, versionName=version_name, type="PRIVATE"
            ),
            f"Omics workflow id={workflow_id} name={base_name} version={version_name}",
        )
    return workflow_id, version_name


def select_existing_workflow_id(logger, omics, name, require_tag=None):
    """
    Find an existing PRIVATE workflow by name (and optional tag),
    skipping DELETED/FAILED.
    If multiple matches, warn and select the first. Logs the choice and returns the id,
    or None if no suitable workflow exists.

    require_tag: tuple (key, value) to require tags[key] == value via get_workflow.
    """
    matches = []
    for page in omics.get_paginator("list_workflows").paginate(
        name=name, type="PRIVATE"
    ):
        for item in page.get("items", []):
            if item.get("status") in ("DELETED", "FAILED"):
                continue
            if require_tag:
                key, val = require_tag
                details = omics.get_workflow(export=[], id=item["id"], type="PRIVATE")
                tags = details.get("tags", {}) or {}
                if key not in tags or tags[key] != val:
                    continue
            matches.append(item)

    if not matches:
        return None

    workflow_id = matches[0]["id"]
    if len(matches) > 1:
        logger.warning(
            "multiple existing Omics workflows named %s; using arbitrary one (%s)",
            name,
            workflow_id,
        )
    return workflow_id


def create_omics_workflow(
    logger,
    cleanup,
    omics,
    workflow_name,
    wdl_doc,
    wdl_exe,
    tags=None,
    *,
    definition_zip=None,
):
    """
    Create a new Omics workflow for this WDL
    """

    wdl_zip = (
        definition_zip
        if definition_zip is not None
        else zip_wdl(logger, cleanup, wdl_doc)
    )
    parameter_template = parameter_template_from_wdl(wdl_exe)

    # create workflow
    logger.debug(
        f"creating Omics workflow {workflow_name} with parameter template: "
        + json.dumps(parameter_template)
    )
    kwargs = dict(
        definitionZip=wdl_zip,
        engine="WDL",
        main=os.path.basename(wdl_doc.pos.abspath),
        name=workflow_name,
        parameterTemplate=parameter_template,
    )
    if tags:
        kwargs["tags"] = tags
    res = omics.create_workflow(**kwargs)
    workflow_id = res["id"]
    logger.info(f"created Omics workflow id={workflow_id} name={workflow_name}")

    return workflow_id


def parameter_template_from_wdl(wdl_exe):
    parameter_template = {}
    for b in wdl_exe.available_inputs:
        parameter_template[b.name] = {
            "description": b.name,  # TODO: get from parameter_meta
            "optional": b.name not in wdl_exe.required_inputs,
        }
    return parameter_template


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


def await_omics_entity(
    logger,
    fetch_fn,
    entity_description: str,
    creating_statuses=("CREATING",),
    failed_statuses=("FAILED",),
):
    """
    Polls fetch_fn() until the entity status is no longer in creating_statuses.
    Exits non-zero if status enters failed_statuses. Logs status messages along the way.
    """
    last_details = None
    while True:
        details = fetch_fn()
        last_details = details
        status = details.get("status")
        msg = f"{entity_description} status {status}, " + details.get(
            "statusMessage", "(no status message)"
        )
        if status in failed_statuses:
            logger.error(msg)
            sys.exit(2)
        logger.debug(msg)
        if status not in creating_statuses:
            break
        time.sleep(1)
    logger.debug("entity details: " + str(last_details))


def await_omics_workflow(logger, omics, workflow_id):
    return await_omics_entity(
        logger,
        lambda: omics.get_workflow(export=[], id=workflow_id, type="PRIVATE"),
        f"Omics workflow {workflow_id}",
    )


def resolve_iam_role_arn(logger, role_name):
    try:
        arn = boto3.client("iam").get_role(RoleName=role_name)["Role"]["Arn"]
        logger.info(f"using IAM role {arn}")
    except Exception as exn:
        logger.exception(exn)
        logger.error(f"unable to resolve IAM role named '{role_name}'")
        sys.exit(1)
    return arn


def resolve_run_group_id(logger, omics, run_group_name):
    groups = omics.list_run_groups(name=run_group_name, maxResults=2)["items"]
    if not groups:
        logger.error(f"no run group named '{run_group_name}'")
        sys.exit(1)
    if len(groups) > 1:
        logger.warning(
            f"multiple run groups named {run_group_name};"
            " supply --run-group-id instead of --run-group to disambiguate"
        )
    id = groups[0]["id"]
    logger.info(f"using run group id={id} name={run_group_name}")
    return id


def resolve_cache_id(logger, omics, cache_name, cache_id):
    if cache_id is not None:
        return cache_id

    existing_count = 0
    existing_id = None
    for page in omics.get_paginator("list_run_caches").paginate():
        for existing in page["items"]:
            if existing["name"] == cache_name:
                existing_count += 1
                existing_id = existing["id"]

    if existing_id is not None:
        if existing_count > 1:
            logger.warning(
                f"multiple existing Omics caches named {cache_name}"
                f"; using arbitrary one ({existing_id})"
            )
        else:
            logger.info(f"using Omics cache id={existing_id} name={cache_name}")
        return existing_id

    logger.error(
        f"no Omics cache named '{cache_name}' found; check name or supply --cache-id"
    )
    sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
