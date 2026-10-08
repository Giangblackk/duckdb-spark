group "default" {
  targets = [ "spark_nessie" ]
}

target "spark_nessie" {
  context = "./"
  dockerfile = "Dockerfile"
  tags = [ "spark_nessie:latest" ]
}