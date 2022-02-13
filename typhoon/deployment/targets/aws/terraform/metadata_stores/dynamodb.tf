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
