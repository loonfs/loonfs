{{/*
The name the Deployment and the Service carry. A release already named after
the chart is used as it stands, so `helm install loonfs-server ...` does not
produce `loonfs-server-loonfs-server`.
*/}}
{{- define "loonfs-server.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
The labels the Deployment's selector matches. A Deployment's selector is
immutable, so nothing that changes between upgrades belongs here.
*/}}
{{- define "loonfs-server.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "loonfs-server.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "loonfs-server.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
The image reference. A digest names one build and wins over the tag. Without
either, the chart runs the server version it shipped with.
*/}}
{{- define "loonfs-server.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else if .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Chart.AppVersion -}}
{{- end -}}
{{- end -}}

{{/*
The Secret holding the config has to exist before the install, and the chart
cannot create it. Say so here rather than rendering a pod that never starts.
*/}}
{{- define "loonfs-server.configSecret" -}}
{{- required "config.existingSecret is required: create the Secret holding your config.toml first, then name it here" .Values.config.existingSecret -}}
{{- end -}}
