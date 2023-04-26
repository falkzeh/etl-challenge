output "mysql_endpoint" {
  description = "The connection endpoint for the RDS MySQL instance"
  value       = aws_db_instance.default.endpoint
}
