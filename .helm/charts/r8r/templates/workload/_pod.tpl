{{/*
Pod Spec
*/}}
{{- define "r8r.pod" -}}
{{- $v := .Values -}}
{{- $global := $v.global -}}
{{- $img := $v.images.r8r -}}
{{- $wl := $v.workload -}}
{{- $main := $v -}}
{{- $pod := $main.pod -}}
{{- $svc := $main.service -}}
{{- $p := $main.persistence -}}
{{- $ctr := $main.containers.r8r -}}

{{- include "common.podSpec" ( dict "podSpec" $pod ) }}
serviceAccountName: sa-{{ include "common.fullname" . }}
imagePullSecrets:
  {{- include "common.imagePullSecrets" ( 
      dict "imagePullSecrets" (concat $global.imagePullSecrets
                                      $pod.imagePullSecrets
                                      $img.imagePullSecrets )) | nindent 2 }}
{{- with (concat $pod.initContainers) }}
initContainers:
  {{- . | toYaml | nindent 2 }} 
{{- end }}
{{- with ( concat $global.extraVolumes
                  $pod.extraVolumes) }}
volumes:
  {{- . | toYaml | nindent 2 }}
{{- end }}
containers:
  {{- with (concat $pod.sidecarContainers) }}
  {{- . | toYaml | nindent 2 }}
  {{- end }}
  - name: r8r
    image: {{ include "common.image" ( dict "img" $img "ctx" $ ) }}
    imagePullPolicy: {{ include "common.firstOf" (
                        dict "items" ( list $global.imagePullPolicy 
                                            $img.imagePullPolicy )) }}
    {{- with $ctr.command }}
    command: 
      {{- . | toYaml | nindent 6 }}
    {{- end }}
    {{- with $ctr.args }}
    args: 
      {{- . | toYaml | nindent 6 }}
    {{- end }}
    {{- with $ctr.securityContext }}
    securityContext: 
      {{- . | toYaml | nindent 6 }}
    {{- end }}
    {{- with $ctr.workingDir }}
    workingDir: {{ . | quote }}
    {{- end }}
    {{- with $ctr.resources }}
    resources:
      {{- . | toYaml | nindent 6 }}
    {{- end }}
    {{- with ( concat $ctr.extraEnvs 
                      $pod.extraEnvs) }}
    env:
      {{- . | toYaml | nindent 6 }}
    {{- end }}
    {{- with ( concat $ctr.extraEnvFrom 
                      $pod.extraEnvFrom) }}
    envFrom:
      {{- . | toYaml | nindent 6 }}
    {{- end }}
    ports:
      {{- range $portName, $portSpecs := $svc.ports }}
      - name: {{ $portName | quote }}
        containerPort: {{ $portSpecs.port }}
        protocol: {{ $portSpecs.protocol }}
        {{- with $portSpecs.hostPort }}
        hostPort: {{ . }}
        {{- end }}
      {{- end }}
    {{- with ( concat $ctr.extraVolumeMounts 
                      $pod.extraVolumeMounts
                      $global.extraVolumeMounts) }}
    volumeMounts:
      {{- . | toYaml | nindent 6 }}
    {{- end }}
    {{- with $ctr.startupProbe }}
    startupProbe:
      {{- . | toYaml | nindent 6 }}
    {{- end }}
    {{- with $ctr.livenessProbe }}
    livenessProbe:
      {{- . | toYaml | nindent 6 }}
    {{- end }}
    {{- with $ctr.readinessProbe }}
    readinessProbe:
      {{- . | toYaml | nindent 6 }}
    {{- end }}
{{- end }}