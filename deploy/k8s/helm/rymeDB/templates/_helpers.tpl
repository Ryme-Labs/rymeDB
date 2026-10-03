{{- define "rymedb.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rymedb.labels" -}}
app.kubernetes.io/name: rymedb
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | default .Chart.Version | quote }}
{{- end -}}

{{- define "rymedb.headless" -}}
{{ include "rymedb.fullname" . }}-headless
{{- end -}}
