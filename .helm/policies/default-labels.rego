package main
import rego.v1

import data.kubernetes

name := input.metadata.name

required_deployment_labels if {
	input.metadata.labels["helm.sh/chart"]
	input.metadata.labels["app.kubernetes.io/name"]
	input.metadata.labels["app.kubernetes.io/instance"]
	input.metadata.labels["app.kubernetes.io/version"]
	input.metadata.labels["app.kubernetes.io/managed-by"]
}

deny contains msg if {
	kubernetes.is_deployment
	not required_deployment_labels
	msg = sprintf("%s must include basic helm labels for objects", [name])
}