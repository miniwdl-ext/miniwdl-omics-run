# miniwdl-omics-run

This command-line tool makes it easier to launch [WDL](https://openwdl.org/) workflow runs on [Amazon Omics](https://docs.aws.amazon.com/omics/latest/dev/workflows.html). It uses [miniwdl](https://github.com/chanzuckerberg/miniwdl) locally to register WDL workflows with the service, validate command-line inputs, and start a run.

```
pip3 install miniwdl-omics-run

miniwdl-omics-run \
    --role-arn {SERVICE_ROLE_ARN} \
    --output-uri s3://{BUCKET_NAME}/{PREFIX} \
    {MAIN_WDL_FILE} input1=value1 input2=value2 ...
```

## Quick start

Prerequisites: up-to-date [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) installed locally, and [configured](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) with full [AdministratorAccess](https://us-east-1.console.aws.amazon.com/iam/home#/policies/arn:aws:iam::aws:policy/AdministratorAccess$jsonEditor) to your AWS account.

### S3 bucket

Create an S3 bucket with a test input file.

```
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_DEFAULT_REGION=$(aws configure get region)

aws s3 mb --region "$AWS_DEFAULT_REGION" "s3://${AWS_ACCOUNT_ID}-${AWS_DEFAULT_REGION}-omics"
echo test | aws s3 cp - s3://${AWS_ACCOUNT_ID}-${AWS_DEFAULT_REGION}-omics/test/test.txt
```

### Service role

Create an IAM service role for your Omics workflow runs to use (to access S3, ECR, etc.).

```
aws iam create-role --role-name poweromics --assume-role-policy-document '{
    "Version":"2012-10-17",
    "Statement":[{
        "Effect":"Allow",
        "Action":"sts:AssumeRole",
        "Principal":{"Service":"omics.amazonaws.com"}
    }]
}'

aws iam attach-role-policy --role-name poweromics \
    --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
```

**WARNING:** PowerUserAccess, suggested here only for brevity, is far more powerful than needed. See [Omics docs on service roles](https://docs.aws.amazon.com/omics/latest/dev/permissions-service.html) for the least privileges necessary, especially if you plan to use third-party WDL and/or Docker images.

### ECR repository

Create an ECR repository suitable for Omics to pull Docker images from.

```
aws ecr create-repository --repository-name omics
aws ecr set-repository-policy --repository-name omics --policy-text '{
    "Version": "2012-10-17",
    "Statement": [{
        "Sid": "omics workflow",
        "Effect": "Allow",
        "Principal": {"Service": "omics.amazonaws.com"},
        "Action": [
            "ecr:GetDownloadUrlForLayer",
            "ecr:BatchGetImage",
            "ecr:BatchCheckLayerAvailability"
        ]
    }]
}'
```

Push a plain Ubuntu image to the repository.

```
ECR_ENDPT="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com"
aws ecr get-login-password | docker login --username AWS --password-stdin "$ECR_ENDPT"

docker pull ubuntu:22.04
docker tag ubuntu:22.04 "${ECR_ENDPT}/omics:ubuntu-22.04"
docker push "${ECR_ENDPT}/omics:ubuntu-22.04"
```

### Run test workflow

```
pip3 install miniwdl-omics-run

miniwdl-omics-run \
    --role-arn arn:aws:iam::${AWS_ACCOUNT_ID}:role/poweromics \
    --output-uri "s3://${AWS_ACCOUNT_ID}-${AWS_DEFAULT_REGION}-omics/test/out" \
    https://raw.githubusercontent.com/miniwdl-ext/miniwdl-omics-run/main/test/TestFlow.wdl \
    input_txt_file="s3://${AWS_ACCOUNT_ID}-${AWS_DEFAULT_REGION}-omics/test/test.txt" \
    docker="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com/omics:ubuntu-22.04"
```

This zips up [the specified WDL](https://raw.githubusercontent.com/miniwdl-ext/miniwdl-omics-run/main/test/TestFlow.wdl), registers it as an Omics workflow, validates the given inputs, and starts the workflow run.

The WDL source code may be set to a local filename or a public HTTP(S) URL. The tool automatically bundles any WDL files imported by the main one. On subsequent invocations, it'll reuse the previously-registered workflow if the source code hasn't changed.

The command-line interface accepts WDL inputs using the `input_key=value` syntax exactly like [`miniwdl run`](https://miniwdl.readthedocs.io/en/latest/runner_cli.html), including the option of a JSON file with `--input FILE.json`. Each input File must be set to an existing S3 URI accessible by the service role.

## Advice

- Omics can use Docker images *only* from your ECR in the same account & region.
  - This often means pulling, re-tagging, and pushing images as illustrated above with `ubuntu:22.04`.
  - And editing any WDL tasks that hard-code public registries in their `runtime.docker`.
  - Each ECR repository must have the Omics-specific repository policy set as shown above.
  - We therefore tend to use a single ECR repository for multiple Docker images, disambiguating them using lengthier tags.
  - If you prefer to use per-image repositories, just remember to set the repository policy on each one.
- To quickly list a workflow's inputs, try `miniwdl run workflow.wdl ?`
