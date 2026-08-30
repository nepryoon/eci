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
{{- $field := printf "global.imageReferences.%s" .workload.name -}}
{{- $image := "" -}}
{{- if .workload.image -}}
{{- $field = printf "applications.workloads[%s].image" .workload.name -}}
{{- $image = .workload.image -}}
{{- else -}}
{{- $image = required (printf "%s must be a registry-issued name@sha256 digest when applications.enabled=true" $field) (index .root.Values.global.imageReferences .workload.name) -}}
{{- end -}}
{{- include "eci.digestImage" (dict "field" $field "value" $image) -}}
{{- end }}

{{- define "eci.digestImage" -}}
{{- $image := required (printf "%s is required" .field) .value -}}
{{- if not (regexMatch "^[^[:space:]@]+@sha256:[0-9a-f]{64}$" $image) -}}
{{- fail (printf "%s must match name@sha256:<64 lowercase hex>, got %q" .field $image) -}}
{{- end -}}
{{- $image -}}
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
