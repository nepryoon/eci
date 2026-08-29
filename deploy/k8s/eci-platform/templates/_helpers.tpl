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
{{- required (printf "global.imageReferences.%s must be a registry-issued name@sha256 digest when applications.enabled=true" .workload.name) (index .root.Values.global.imageReferences .workload.name) -}}
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
