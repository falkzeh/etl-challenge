resource "aws_vpc" "default" {
  cidr_block = "10.0.0.0/16"

  tags = {
    Name = "${var.name}"
    Environment = "${var.environment}"
  }
}

resource "aws_security_group" "default" {
  name        = "mysql-sg"
  description = "MySQL security group"
  vpc_id      = aws_vpc.default.id

  ingress {
    from_port   = 3306
    to_port     = 3306
    protocol    = "tcp"
    cidr_blocks = ["${var.cidr_block}"]
  }
}

resource "aws_db_subnet_group" "default" {
  name       = "main-subnet-group"
  subnet_ids = "${aws_subnet.private.*.id}"

  tags = {
    Name = "${var.name}"
    Environment = "${var.environment}"
  }
}

resource "aws_subnet" "private" {
  count = 2

  cidr_block = "10.0.${count.index + 1}.0/24"
  vpc_id      = aws_vpc.default.id

  tags = {
    Name = "${var.name}"
    Environment = "${var.environment}"
  }
}

resource "aws_db_instance" "default" {
  identifier        = "${var.name}-db"
  allocated_storage = 20
  storage_type      = "gp2"
  engine            = "mysql"
  engine_version    = "8.0"
  instance_class    = "db.t2.micro"
  username          = var.db_username
  password          = var.db_password
  vpc_security_group_ids = [aws_security_group.default.id]
  db_subnet_group_name   = aws_db_subnet_group.default.name

  skip_final_snapshot = true

  tags = {
    Name = "${var.name}"
    Environment = "${var.environment}"
  }
}
