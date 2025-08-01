{{/*
Expand the name of the chart.
*/}}
{{- define "buildfarm.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "buildfarm.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "buildfarm.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "buildfarm.labels" -}}
helm.sh/chart: {{ include "buildfarm.chart" . }}
{{ include "buildfarm.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "buildfarm.selectorLabels" -}}
app.kubernetes.io/name: {{ include "buildfarm.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "buildfarm.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "buildfarm.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the APIsersion of the holizontal pod autoscaler
*/}}
{{- define "buildfarm.autoscalingVersion" -}}
{{- if (.Capabilities.APIVersions.Has "autoscaling/v2") -}}
autoscaling/v2
{{- else if (.Capabilities.APIVersions.Has "autoscaling/v2beta2") -}}
autoscaling/v2beta2
{{- else -}}
autoscaling/v1
{{- end -}}
{{- end -}}

{{/* Checks for `externalRedis` */}}
{{- if .Values.externalRedis.host }}
  {{/* check if they are using externalRedis (the default value for `externalRedis.host` is "localhost") */}}
  {{- if not (eq .Values.externalRedis.host "localhost") }}
    {{- if .Values.redis.enabled }}
    {{ required "If `externalRedis.host` is set, then `redis.enabled` should be `false`!" nil }}
    {{- end }}
  {{- end }}
{{- end }}
