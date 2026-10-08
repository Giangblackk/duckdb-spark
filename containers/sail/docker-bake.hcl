group "default" {
  targets = [ "sail" ]
}

target "sail" {
  context = "./"
  dockerfile = "Dockerfile"
  tags = [ "sail:latest" ]
  args = {
    PYSAIL_VERSION = "0.7.2"
  }
}