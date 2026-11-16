group "default" {
  targets = [ "spark_sedona" ]
}

target "spark_sedona" {
  context = "./"
  dockerfile = "Dockerfile"
  tags = [ "spark_sedona:latest" ]
}