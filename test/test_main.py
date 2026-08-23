import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch

from botocore.session import get_session
from botocore.validate import validate_parameters

import miniwdl_omics_run.__main__ as cli


class VpcConfigTests(unittest.TestCase):
    def parse(self, *args):
        return cli.arg_parser().parse_args(args)

    def test_vpc_config_adds_start_run_networking_options(self):
        args = self.parse("workflow.wdl", "--vpc-config", "my-vpc-config")

        self.assertEqual(
            cli.start_run_options(args),
            {
                "networkingMode": "VPC",
                "configurationName": "my-vpc-config",
            },
        )

    def test_vpc_config_payload_matches_start_run_model(self):
        args = self.parse("workflow.wdl", "--vpc-config", "my-vpc-config")
        payload = {
            "outputUri": "s3://bucket/output",
            "parameters": {},
            "roleArn": "arn:aws:iam::123456789012:role/omics",
            "workflowId": "1234567",
            "workflowType": "PRIVATE",
            "logLevel": "ALL",
            "requestId": "request-id",
            **cli.start_run_options(args),
        }

        operation = get_session().get_service_model("omics").operation_model("StartRun")
        validate_parameters(payload, operation.input_shape)

    def test_without_vpc_config_omits_networking_options(self):
        args = self.parse("workflow.wdl")
        options = cli.start_run_options(args)

        self.assertNotIn("networkingMode", options)
        self.assertNotIn("configurationName", options)

    def test_build_rejects_vpc_config(self):
        document = SimpleNamespace(workflow=object(), tasks=[])

        with (
            patch.object(cli.WDL, "load", return_value=document),
            patch.object(cli, "configure_logger", return_value=nullcontext()),
            patch.object(cli.boto3, "client") as client,
        ):
            with self.assertRaises(SystemExit) as exit_info:
                cli.main(
                    [
                        "miniwdl-omics-run",
                        "--build",
                        "--vpc-config",
                        "my-vpc-config",
                        "workflow.wdl",
                    ]
                )

        self.assertEqual(exit_info.exception.code, 1)
        client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
