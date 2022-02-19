terraform {
  experiments = [module_variable_optional_attrs]
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
variable "dag_info" {
  type = map(object({
    schedule_interval = string
    environment = optional(map(string))
  }))
}
variable "runtime" { default = "python3.6" }
variable "deployment_bucket" { type = string }
variable "metadata_db_url" {
  type = string
  default = ""
}
variable "metadata_suffix" {
  type = string
  default = ""
}
variable "project_name" { type = string }

resource "aws_lambda_function" "dag" {
  for_each = var.dag_info
  function_name    = "${each.key}_${var.env}"
  role             = "${aws_iam_role.lambda_exec_role[each.key].arn}"
  handler          = "${each.key}.${each.key}_main"
  runtime          = "${var.runtime}"
  s3_bucket = "${var.deployment_bucket}"
  s3_key = "typhoon_dag_builds/${each.key}/lambda.zip"
  layers = [aws_lambda_layer_version.dag_dependencies_layer[each.key].arn]
  timeout = 30
  environment {
    variables = merge({
      "TYPHOON_METADATA_DB_URL" = var.metadata_db_url,
      "TYPHOON_METADATA_SUFFIX" = var.metadata_suffix
      "TYPHOON_PROJECT_NAME" = var.project_name
      "TYPHOON_HOME" = "/var/task"
    }, each.value["environment"])
  }
}

resource "aws_lambda_layer_version" "dag_dependencies_layer" {
  for_each = var.dag_info
  s3_bucket = "${var.deployment_bucket}"
  s3_key = "typhoon_dag_builds/${each.key}/layer.zip"
  layer_name = "${each.key}_dependencies"

  compatible_runtimes = [var.runtime]
}

resource "aws_iam_role" "lambda_exec_role" {
  for_each = var.dag_info
  name        = "lambda_exec_${each.key}"
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

resource "aws_iam_policy" "invoke_dag" {
  for_each = var.dag_info
  name        = "invoke_dag_${each.key}"
  description = "Policy for a lambda DAG to invoke itself"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        "Effect": "Allow",
        "Action": "lambda:InvokeFunction",
        "Resource": aws_lambda_function.dag[each.key].arn 
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_invoke_dag" {
  for_each = var.dag_info
  role       = aws_iam_role.lambda_exec_role[each.key].name
  policy_arn = aws_iam_policy.invoke_dag[each.key].arn
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
