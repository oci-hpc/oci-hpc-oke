{{- define "oci-quickcache.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "oci-quickcache.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "oci-quickcache.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "oci-quickcache.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "oci-quickcache.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "oci-quickcache.selectorLabels" -}}
app.kubernetes.io/name: {{ include "oci-quickcache.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
