variable "VERSION" {
  default = "0.0.1"
}

variable "REGISTRY" {
  default = "docker.io"
}

variable "IMAGE" {
  default = "gglamer/ingest-bridge"
}

target "_base" {
  context    = "."
  dockerfile = "Dockerfile"
  tags = [
    "${REGISTRY}/${IMAGE}:${VERSION}",
    "${REGISTRY}/${IMAGE}:latest",
  ]
  cache-from = ["type=registry,ref=${REGISTRY}/${IMAGE}:cache"]
  cache-to   = ["type=inline"]
}

target "local" {
  inherits = ["_base"]
  output   = ["type=docker"]
}

target "release" {
  inherits  = ["_base"]
  platforms = ["linux/amd64", "linux/arm64"]
  output    = ["type=registry"]
}
