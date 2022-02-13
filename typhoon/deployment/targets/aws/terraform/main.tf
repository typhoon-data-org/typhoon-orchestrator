terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = "eu-west-1"
}

variable "env" { type = string }
variable "dag_info" { type = map }
variable "runtime" { default = "python3.6" }
variable "deployment_bucket" { type = string }
variable "metadata_db_url" { type = string }
variable "metadata_suffix" { type = string }

resource "aws_lambda_function" "dag" {
  for_each = var.dag_info
  role             = "${aws_iam_role.lambda_exec_role.arn}"
  handler          = "${each.key}_main"
  runtime          = "${var.runtime}"
  s3_bucket = "${var.deployment_bucket}"
  s3_key = "typhoon_dag_builds/${each.key}.zip"
  function_name    = "${each.key}_${var.env}"
  environment {
    variables = {
      "TYPHOON_METADATA_DB_URL" = var.metadata_db_url,
      "TYPHOON_METADATA_SUFFIX" = var.metadata_suffix
    }
  }
}

resource "aws_iam_role" "lambda_exec_role" {
  name        = "lambda_exec"
  path        = "/"
  description = "Allows Lambda Function to call AWS services on your behalf."

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_cloudwatch_event_rule" "dag_trigger" {
  for_each = var.dag_info
  name                = "${each.key}_trigger"
  description         = "Fires ${each.key} dag"
  schedule_expression = each.value["schedule_interval"]
}

resource "aws_cloudwatch_event_target" "trigger_event_target" {
  for_each = var.dag_info
  rule      = aws_cloudwatch_event_rule.dag_trigger[each.key].name
  target_id = "lambda"
  arn       = aws_lambda_function.dag[each.key].arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_dag" {
  for_each = var.dag_info
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.dag[each.key].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.dag_trigger[each.key].arn
}
