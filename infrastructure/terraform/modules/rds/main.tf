resource "aws_db_subnet_group" "main" {
  name       = "${var.name_prefix}-rds-subnet-group"
  subnet_ids = var.private_subnet_ids
}

resource "aws_security_group" "rds" {
  name   = "${var.name_prefix}-rds-sg"
  vpc_id = var.vpc_id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }
}

resource "random_password" "db_password" {
  length  = 32
  special = false
}

resource "aws_db_instance" "main" {
  identifier        = "${var.name_prefix}-postgres"
  engine            = "postgres"
  engine_version    = "15.4"
  instance_class    = var.instance_class
  allocated_storage = var.allocated_storage
  storage_encrypted = true

  db_name  = "finstreami"
  username = "finstreami"
  password = random_password.db_password.result

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  multi_az               = var.environment == "prod"
  backup_retention_period = 7
  deletion_protection    = var.environment == "prod"
  skip_final_snapshot    = var.environment != "prod"

  parameter_group_name = aws_db_parameter_group.main.name
}

resource "aws_db_parameter_group" "main" {
  name   = "${var.name_prefix}-pg15"
  family = "postgres15"

  parameter {
    name  = "shared_preload_libraries"
    value = "pg_stat_statements"
  }
}
