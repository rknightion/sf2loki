{{/*
Shared template helpers for the sf2loki chart.

sf2loki is a single-instance-by-default service (active-passive HA via a k8s Lease when
ha.enabled — the Pub/Sub API has no consumer-group semantics, so >1 concurrent subscriber
double-delivers). The chart deploys ONE release into one namespace; names derive from the
release via the standard fullname helper (override with nameOverride/fullnameOverride).

IMPORTANT: sf2loki.selectorLabels is the immutable Deployment .spec.selector / PDB selector.
Keep it a stable, minimal subset — never fold version/chart labels into it, or a `helm upgrade`
over an existing Deployment fails (selector is immutable post-create).
*/}}

{{- define "sf2loki.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sf2loki.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "sf2loki.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sf2loki.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sf2loki.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "sf2loki.labels" -}}
helm.sh/chart: {{ include "sf2loki.chart" . }}
{{ include "sf2loki.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: sf2loki
{{- end -}}

{{/* ServiceAccount name (created one, or an override). */}}
{{- define "sf2loki.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "sf2loki.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Name of the Secret holding the mounted secret files (created or referenced). */}}
{{- define "sf2loki.secretName" -}}
{{- if .Values.secrets.existingSecret -}}
{{- .Values.secrets.existingSecret -}}
{{- else -}}
{{- printf "%s-secrets" (include "sf2loki.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* Fully-resolved image reference: repository + (digest wins over tag; tag defaults to appVersion). */}}
{{- define "sf2loki.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
{{- end -}}
