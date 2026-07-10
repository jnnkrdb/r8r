# [r8r] Replicator

![Go Version](https://img.shields.io/badge/go-1.26-00ADD8?logo=go)
[![CodeFactor](https://www.codefactor.io/repository/github/jnnkrdb/r8r/badge)](https://www.codefactor.io/repository/github/jnnkrdb/r8r)
![License](https://img.shields.io/github/license/jnnkrdb/r8r)
![Status](https://img.shields.io/badge/status-experimental-orange)

**r8r** is a Kubernetes operator that allows you to create and manage
namespaced resources across multiple namespaces from a single source of truth.
Target namespaces are selected via labels.

The main goal of **r8r** is to simplify multi-namespace and multi-tenant setups
by avoiding duplicated YAML manifests.

## General

### Key Features

- **Single Source of Truth** for namespaced resources
- **Label-based namespace selection**
- **Automatic reconciliation** across namespaces
- Works with **native Kubernetes resources** (ConfigMaps, Secrets, etc.)
- Built using **Go** and **kubebuilder / controller-runtime**
- Designed to be **extensible and declarative**

### Core Concept

In Kubernetes clusters with many namespaces (e.g. per team, tenant, or environment), it is common to duplicate the same resources across namespaces:

- ConfigMaps
- Secrets
- NetworkPolicies
- Custom Resources

This leads to:

- duplicated YAML
- configuration drift
- error-prone manual updates

**r8r** solves this by allowing you to define a resource **once** and automatically replicate it in **all matching namespaces**.

**r8r** addresses this by:

1. defining resources once as a **ClusterObject**
2. selecting target namespaces via labels
3. automatically creating and reconciling those resources in all matching namespaces

### Installation

Install via Helm:

```bash
helm upgrade --install r8r oci://ghcr.io/jnnkrdb/r8r --version {version}
```

## Example Use Case

Now here is a little example on how to use this operator. Think of having 3 different applications (**app-a**, **app-b**, **app-c**), which all are accross 3 different namespaces each (**dev**, **test**, **prod**). With this setup you will have 9 different namespaces. 

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: app-a
  labels:
    environment: dev
    ips-default: "true"
---
apiVersion: v1
kind: Namespace
metadata:
  name: app-a
  labels:
    environment: test
    ips-default: "true"
---
apiVersion: v1
kind: Namespace
metadata:
  name: app-a
  labels:
    environment: prod
    ips-default: "true"
---
...
```

Every namespace now needs the same imagepullsecret, since all applications come from the same registry.
Under normal cirumstances you would have to create 9 Secrets inside of the 9 namespace and manage each of the secret manually.

With **r8r** you only have to create one single source of truth. In this case the ClusterObject contains a Secret as a resource:

```yaml
apiVersion: cluster.jnnkrdb.de/v1alpha1
kind: ClusterObject
metadata:
  name: default-image-pull-secrets
replicator:
  labelSelector:
    matchLabels:
      ips-default: "true"
  resource:
    apiVersion: v1
    kind: Secret
    metadata:
      name: default-ips
    type: kubernetes.io/dockerconfigjson
    data:
      .dockerconfigjson: |-
        "..."
```

The above created **ClusterObject** now replicates the configured secret into all namespaces  with the label `ips-default: "true"`.
If now something regarding the ImagePullSecrets changes, you just have to change the value in the **ClusterObject** and **r8r** will synchronize it into the required namespaces.

```ATTENTION:``` This approach only deploys the corresponding resource, if it does not already exist. If there already is a secret with the name `default-ips` in a namespace, which matches the labelselector, then the reconciliation will be skipped and ignore the namespace.

### Reconciliation Behavior

The controller continuously ensures that:

- resources exist in all matching namespaces
- resources are updated when the source changes
- newly labelled namespaces receive the resource
- removed namespaces stop being managed

### Limitations

- No per-namespace overrides
- Conflict handling is minimal
- API may change without notice

## Roadmap (Ideas)

- Status reporting per namespace
- Dry-run mode
- Better conflict detection
- Graphical User Interface
  - Label Calculation test
  - General overview of replicated objects + status
- `ignore-namespace` Annotations ([#51](https://github.com/jnnkrdb/r8r/issues/51))
- Delayed Syncs ([#50](https://github.com/jnnkrdb/r8r/issues/50))
