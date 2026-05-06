resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.name_prefix}-kafka"
  kafka_version          = "3.4.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_storage_gb
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }
}

resource "aws_msk_configuration" "main" {
  name          = "${var.name_prefix}-kafka-config"
  kafka_versions = ["3.4.0"]

  server_properties = <<-PROPS
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=10
    log.retention.hours=168
  PROPS
}

resource "aws_security_group" "msk" {
  name   = "${var.name_prefix}-msk-sg"
  vpc_id = var.vpc_id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }

  ingress {
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }
}
