resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.name_prefix}-data-lake-${var.project_name}"
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  # Bronze tier — raw data, move to IA after 30 days
  rule {
    id     = "bronze-lifecycle"
    status = "Enabled"
    filter { prefix = "bronze/" }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  # Silver tier — processed data
  rule {
    id     = "silver-lifecycle"
    status = "Enabled"
    filter { prefix = "silver/" }

    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
  }

  # Gold tier — curated data, keep in STANDARD longer
  rule {
    id     = "gold-lifecycle"
    status = "Enabled"
    filter { prefix = "gold/" }

    transition {
      days          = 180
      storage_class = "STANDARD_IA"
    }
  }
}
