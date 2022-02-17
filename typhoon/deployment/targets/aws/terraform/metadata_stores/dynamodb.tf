variable "connections_table" { type = string }
variable "variables_table" { type = string }
variable "dag_deployments_table" { type = string }

resource "aws_dynamodb_table" "connections" {
  name              = "${var.connections_table}"
  read_capacity     = 1
  write_capacity    = 1
  hash_key          = "conn_id"

  attribute {
    name = "conn_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "variables" {
  name              = "${var.variables_table}"
  read_capacity     = 1
  write_capacity    = 1
  hash_key          = "id"

  attribute {
    name = "id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "dag_deployments" {
  name              = "${var.dag_deployments_table}"
  read_capacity     = 1
  write_capacity    = 1
  hash_key          = "deployment_hash"
  range_key         = "deployment_date"

  attribute {
    name = "deployment_hash"
    type = "S"
  }

  attribute {
    name = "deployment_date"
    type = "S"
  }
}

resource "aws_iam_policy" "read_dynamodb_tables" {
  for_each = var.dag_info
  name        = "access_typhoon_tables_${each.key}"
  description = "Policy for a lambda DAG to invoke itself"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        "Sid": "AccessTyphoonConnections${each.key}",
        "Effect": "Allow",
        "Action": [
            "dynamodb:BatchGet*",
            "dynamodb:DescribeTable",
            "dynamodb:Get*",
            "dynamodb:Query",
            "dynamodb:Scan",
            "dynamodb:BatchWrite*",
            "dynamodb:Update*",
            "dynamodb:PutItem"
        ],
        "Resource": aws_dynamodb_table.connections.arn
      },
      {
        "Sid": "AccessTyphoonVariables${each.key}",
        "Effect": "Allow",
        "Action": [
            "dynamodb:BatchGet*",
            "dynamodb:DescribeTable",
            "dynamodb:Get*",
            "dynamodb:Query",
            "dynamodb:Scan",
            "dynamodb:BatchWrite*",
            "dynamodb:Update*",
            "dynamodb:PutItem"
        ],
        "Resource": aws_dynamodb_table.variables.arn
      },
      {
        "Sid": "AccessTyphoonDagDeployments${each.key}",
        "Effect": "Allow",
        "Action": [
            "dynamodb:BatchGet*",
            "dynamodb:DescribeTable",
            "dynamodb:Get*",
            "dynamodb:Query",
            "dynamodb:Scan",
            "dynamodb:BatchWrite*",
            "dynamodb:Update*",
            "dynamodb:PutItem"
        ],
        "Resource": aws_dynamodb_table.dag_deployments.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_read_tables" {
  for_each = var.dag_info
  role       = aws_iam_role.lambda_exec_role[each.key].name
  policy_arn = aws_iam_policy.read_dynamodb_tables[each.key].arn
}
