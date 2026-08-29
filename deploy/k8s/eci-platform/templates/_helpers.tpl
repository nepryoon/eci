{{- define "eci.labels" -}}
app.kubernetes.io/part-of: eci
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- define "eci.selectorLabels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/component: {{ .name }}
app.kubernetes.io/part-of: eci
{{- end }}

{{- define "eci.image" -}}
{{- if .workload.image -}}
{{ .workload.image }}
{{- else -}}
{{ $.root.Values.global.imageRegistry }}/{{ .workload.name }}:{{ $.root.Values.global.imageTag }}
{{- end -}}
{{- end }}

{{- define "eci.containerSecurityContext" -}}
allowPrivilegeEscalation: false
capabilities:
  drop: ["ALL"]
readOnlyRootFilesystem: true
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
{{- end }}

{{- define "eci.podSecurityContext" -}}
runAsNonRoot: true
seccompProfile:
  type: RuntimeDefault
{{- end }}
