{{- define "telemetry.name" -}}
{{- printf "%s-telemetry" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- define "telemetry.validate" -}}
{{- $image := required "image is required" .Values.image -}}
{{- if .Values.production -}}
{{- if not (regexMatch "@sha256:[a-f0-9]{64}$" $image) -}}{{ fail "production image must be pinned by sha256 digest" }}{{- end -}}
{{- if not (hasPrefix "https://" .Values.runtime.schemaRegistryUrl) -}}{{ fail "production registry requires HTTPS" }}{{- end -}}
{{- if not (hasPrefix "https://" .Values.runtime.issuer) -}}{{ fail "production issuer requires HTTPS" }}{{- end -}}
{{- if not (has .Values.runtime.kafkaSecurityProtocol (list "SSL" "SASL_SSL")) -}}{{ fail "production Kafka requires TLS" }}{{- end -}}
{{- if empty .Values.networkPolicy.ingressPeers -}}{{ fail "production requires explicit application ingress peers" }}{{- end -}}
{{- if not .Values.networkPolicy.enabled -}}{{ fail "production requires networkPolicy.enabled" }}{{- end -}}
{{- if empty .Values.networkPolicy.egress -}}{{ fail "production requires explicit dependency egress rules" }}{{- end -}}
{{- if lt (int .Values.collector.replicas) 2 -}}{{ fail "production requires at least two collectors" }}{{- end -}}
{{- if lt (int .Values.worker.replicas) 2 -}}{{ fail "production requires at least two workers" }}{{- end -}}
{{- end -}}
{{- $_ := required "kafkaBootstrapServers is required" .Values.runtime.kafkaBootstrapServers -}}
{{- end -}}
