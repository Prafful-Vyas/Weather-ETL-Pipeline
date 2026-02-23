terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  # Configuration options
  region = "us-east-1"
}

# Create a S3 bucket
resource "aws_s3_bucket" "pvyas_bucket" {
  bucket = "pvyas-test-bucket"

  tags = {
    Name        = "My bucket 2.0"
    Environment = "Dev"
  }
}

